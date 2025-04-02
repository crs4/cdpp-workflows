import json
import logging
import os
import shutil
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse
from uuid import uuid4

import requests
import yaml
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import DagRun, Variable
from airflow.operators.python import get_current_context
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

logger = logging.getLogger()

_copy_registry = {}
_move_registry = {}
_remove_registry = {}


OME_SEADRAGON_REGISTER_SLIDE = Variable.get("OME_SEADRAGON_REGISTER_SLIDE")
OME_SEADRAGON_URL = Variable.get("OME_SEADRAGON_URL")

PROMORT_CONNECTION = BaseHook.get_connection("promort")
PROMORT_SESSION_ID = Variable.get("PROMORT_SESSION_ID")

PREDICTIONS_DIR = Variable.get("PREDICTIONS_DIR")
INPUT_DIR = Variable.get("INPUT_DIR")
STAGE_DIR = Variable.get("STAGE_DIR")
FAILED_DIR = Variable.get("FAILED_DIR")
BACKUP_DIR = Variable.get("BACKUP_DIR")

DOCKER_NETWORK = Variable.get("DOCKER_NETWORK", default_var="")

PROMORT_TOOLS_IMG = Variable.get("PROMORT_TOOLS_IMG")

PROVENANCE = Variable.get("PROVENANCE", default_var=False)


def handle_error(ctx):
    slide = ctx["params"]["slide"]
    copy_slide(os.path.join(STAGE_DIR, slide), FAILED_DIR)
    #  _remove_slide_from_omero(slide)


def copy_slide(slide_path: str, dest: str):
    _slide_path = Path(slide_path)
    _dest = Path(dest)
    return _copy_registry.get(_slide_path.suffix[1:], _copy_file)(
        _slide_path,
        _dest,
    )


def move_slide(slide_path: str, dest: str):
    _slide_path = Path(slide_path)
    _dest = Path(dest)
    _move_registry.get(_slide_path.suffix[1:], _move_file)(
        _slide_path,
        _dest,
    )


def remove_slide(slide_path: str):
    _slide_path = Path(slide_path)
    _remove_registry.get(_slide_path.suffix[1:], _remove_file)(_slide_path)


def _copy_file(slide_path: Path, dest_dir: Path):
    dest_path = Path(dest_dir, slide_path.name)
    shutil.copy(
        slide_path.absolute().as_posix(),
        dest_path.absolute().as_posix(),
    )


def _copy_mrxs(slide_path: Path, dest_dir: Path):
    dir_dest_path = Path(dest_dir, slide_path.stem)
    shutil.copytree(
        Path(slide_path.parent.absolute(), slide_path.stem).absolute().as_posix(),
        dir_dest_path.absolute().as_posix(),
    )
    _copy_file(slide_path, dest_dir)


def _move_file(slide_path: Path, dest_dir: Path):
    dest_path = Path(dest_dir, slide_path.name)
    shutil.move(
        slide_path.absolute().as_posix(),
        dest_path.absolute().as_posix(),
    )


def _move_mrxs(slide_path: Path, dest_dir: Path):
    dir_dest_path = Path(dest_dir, slide_path.stem)

    shutil.move(
        Path(slide_path.parent.absolute(), slide_path.stem).absolute().as_posix(),
        dir_dest_path.absolute().as_posix(),
    )
    _move_file(slide_path, dest_dir)


def _remove_file(slide_path: Path):
    os.remove(slide_path)


def _remove_mrxs(slide_path: Path):
    os.remove(slide_path)
    shutil.rmtree(
        Path(slide_path.parent.absolute(), slide_path.stem), ignore_errors=True
    )


_copy_registry["mrxs"] = _copy_mrxs
_move_registry["mrxs"] = _move_mrxs
_remove_registry["mrxs"] = _remove_mrxs


def check_gpus_available(gpus):
    logger.info("checking availibility for gpus %s", gpus)
    if gpus:
        cmd = f"--pid=host --gpus={gpus} ubuntu:20.04 bash -c nvidia-smi | grep ' C ' | wc -l"
        gpu_processes = docker_run(cmd)
        if int(gpu_processes):
            raise RuntimeError(f"processes already running on gpu(s) {gpus}")


def _run(command, shell=False):
    logger.info(
        "command %s", " ".join(command) if isinstance(command, list) else command
    )
    res = subprocess.run(command, capture_output=True, shell=shell)
    if res.returncode:
        logger.error(res.stderr)
        res.check_returncode()

    out = res.stdout.decode()
    logger.info("out %s", out)
    return out


def docker_run(command, network: Optional[str] = None):
    docker_cmd = ["docker", "run", "--rm"]
    if network:
        docker_cmd.append("--network")
        docker_cmd.append(network)
    command = (
        docker_cmd + command
        if isinstance(command, list)
        else " ".join(docker_cmd) + f" {command}"
    )
    return _run(command, shell=isinstance(command, str))


@task
def prepare_data():
    slide = get_current_context()["params"]["slide"]
    try:
        copy_slide(os.path.join(INPUT_DIR, slide), BACKUP_DIR)
    except FileExistsError:
        logger.info("slide %s already in backup", slide)
    except Exception as ex:
        logger.error("failed to backup slide, is it a re-run? Ex: %s", ex)

    try:
        move_slide(os.path.join(INPUT_DIR, slide), STAGE_DIR)
    except shutil.Error:
        logger.info("slide already in stage, removing from input dir")
        try:
            remove_slide(os.path.join(INPUT_DIR, slide))
        except Exception as ex:
            logger.error(
                "cannot remove slide from input dir, is it a re-run? Ex %s", ex
            )

    return slide


@task(multiple_outputs=True)
def add_slide_to_omero(slide) -> Dict[str, str]:
    slide_name = os.path.splitext(slide)[0]
    response = requests.get(
        OME_SEADRAGON_REGISTER_SLIDE, params={"slide_name": slide_name}
    )

    logger.info("response.text %s", response.text)
    response.raise_for_status()
    omero_id = response.json()["mirax_index_omero_id"]

    return {"slide": slide_name, "omero_id": omero_id}


@task
def processing(cwl_dag_id) -> Dict[str, str]:

    global_params = get_current_context()["params"]
    slide = global_params["slide"]
    # cwl_dag_id = global_params.get("processing_workflow", "pca_classification")
    allowed_states = [State.SUCCESS]
    failed_states = [State.FAILED]

    slide_param = {"slide": {"class": "File", "path": slide}}
    input_params = global_params["params"]
    input_params.update(slide_param)

    if cwl_dag_id == "pca_classification":
        mode = input_params.get("mode") or Variable.get("PREDICTIONS_MODE")
        if mode == "serial":
            params = Variable.get("SERIAL_PREDICTIONS_PARAMS", deserialize_json=True)
        else:
            params = Variable.get("PARALLEL_PREDICTIONS_PARAMS", deserialize_json=True)
    else:
        params = {}

    logger.info("default params %s, input_params %s", params, input_params)
    params.update(input_params)

    if "gpu" in input_params:
        gpus = os.environ["CWLDOCKER_GPUS"]
        check_gpus_available(gpus)

    execution_date = timezone.utcnow()
    triggered_run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)
    triggered_run_id = f"{slide}-{triggered_run_id}"

    conf = {"job": params}
    logger.info(
        "triggering dag with id %s, run_id %s, conf %s", cwl_dag_id, triggered_run_id, conf
    )

    dag_run = trigger_dag(
        dag_id=cwl_dag_id,
        run_id=triggered_run_id,
        execution_date=execution_date,
        conf=conf,
        replace_microseconds=False,
    )
    while True:
        time.sleep(10)

        dag_run.refresh_from_db()
        state = dag_run.state
        if state in failed_states:
            raise AirflowException(f"{cwl_dag_id} failed with failed states {state}")
        if state in allowed_states:
            return {"dag_id": cwl_dag_id, "dag_run_id": triggered_run_id}


@task
def add_slide_to_promort(slide_info: Dict[str, str]):

    slide = os.path.splitext(slide_info["slide"])[0]
    omero_id = slide_info["omero_id"]
    command = [
        PROMORT_TOOLS_IMG,
        "importer.py",
        "--host",
        f"{PROMORT_CONNECTION.conn_type}://{PROMORT_CONNECTION.host}:{PROMORT_CONNECTION.port}",
        "--user",
        PROMORT_CONNECTION.login,
        "--passwd",
        PROMORT_CONNECTION.password,
        "--session-id",
        PROMORT_SESSION_ID,
        "slides_importer",
        "--slide-label",
        slide,
        "--extract-case",
        "--omero-id",
        str(omero_id),
        "--mirax",
        "--omero-host",
        OME_SEADRAGON_URL,
        "--ignore-duplicated",
    ]
    docker_run(command, DOCKER_NETWORK)


def add_prediction_to_omero(prediction, dag_info) -> Dict[str, str]:
    dag_id, dag_run_id = dag_info["dag_id"], dag_info["dag_run_id"]
    logger.info(
        "register prediction %s to omero with dag_id %s, dag_run_id %s",
        prediction.value,
        dag_id,
        dag_run_id,
    )
    output_dir = _get_output_dir(dag_id, dag_run_id)
    location = _get_prediction_location(prediction, output_dir)
    dest = _move_prediction_to_omero_dir(location)
    return _register_prediction_to_omero(
        os.path.basename(dest), prediction == Prediction.TUMOR
    )


def add_prediction_to_promort(
    prediction,
    slide_label: str,
    prediction_label: str,
    omero_id: str,
    report_dir: Optional[str] = None,
    review_required: bool = False,
) -> str:

    command = [
        PROMORT_TOOLS_IMG,
        "importer.py",
        "--host",
        f"{PROMORT_CONNECTION.conn_type}://{PROMORT_CONNECTION.host}:{PROMORT_CONNECTION.port}",
        "--user",
        PROMORT_CONNECTION.login,
        "--passwd",
        PROMORT_CONNECTION.password,
        "--session-id",
        PROMORT_SESSION_ID,
        "predictions_importer",
        "--prediction-label",
        prediction_label,
        "--slide-label",
        slide_label,
        "--prediction-type",
        prediction.upper(),
        "--omero-id",
        omero_id,
    ]
    if review_required:
        command.append("--review-required")
    if report_dir and PROVENANCE:
        provenance = docker_run(
            [
                "-v",
                f"{report_dir}:/data",
                "dh/provenance",
                prediction,
                "--workflow-path",
                "/data/predictions.cwl",
                "--params-path",
                "/data/params.json",
                "--dates-path",
                "/data/dates.json",
            ]
        )
        command += ["--provenance", provenance]

    logger.info("command %s", command)
    res = docker_run(command, DOCKER_NETWORK)
    return json.loads(res)["id"]


@task
def convert_to_tiledb(dataset_label):

    command = [
        "-v",
        f"{PREDICTIONS_DIR}:/data",
        PROMORT_TOOLS_IMG,
        "zarr_to_tiledb.py",
        "--zarr-dataset",
        f"/data/{dataset_label}",
        "--out-folder",
        "/data",
    ]
    return docker_run(command, DOCKER_NETWORK)


class Prediction(Enum):
    TISSUE = "tissue"
    TUMOR = "tumor"


def _get_output_dir(dag_id, dag_run_id):
    return (
        os.path.join(Variable.get("OUT_DIR"), dag_id, dag_run_id)
        .replace(":", "_")
        .replace("+", "_")
    )


def _move_prediction_to_omero_dir(location):
    dest = os.path.join(PREDICTIONS_DIR, f"{str(uuid4())}.zip")
    logger.info("moving %s to %s", location, dest)
    # @fixme change to move
    shutil.copy(location, dest)
    return dest


def _register_prediction_to_omero(label, extract_archive) -> Dict[str, str]:
    logger.info(
        "register_prediction_to_omero: label %s, extract_archive %s",
        label,
        extract_archive,
    )
    ome_seadragon_register_predictions = Variable.get(
        "OME_SEADRAGON_REGISTER_PREDICTIONS"
    )

    response = requests.get(
        ome_seadragon_register_predictions,
        params={
            "dataset_label": label,
            "keep_archive": True,
            "extract_archive": extract_archive,
        },
    )
    response.raise_for_status()
    res_json = response.json()

    logger.info(res_json)
    return res_json


def _get_prediction_location(prediction, output_dir):
    with open(os.path.join(output_dir, "workflow_report.json"), "r") as report_file:
        report = json.load(report_file)

    return report[prediction.value]["location"].replace("file://", "")


@task
def gather_report(dag_info, cwl_workflow):
    params_fn = "params.json"
    metadata_fn = "metadata.yaml"

    dag_id, dag_run_id = dag_info["dag_id"], dag_info["dag_run_id"]
    output_dir = _get_output_dir(dag_id, dag_run_id)
    with open(os.path.join(output_dir, "workflow_report.json")) as f:
        airflow_report = json.load(f)

    global_params = get_current_context()["params"]
    orig_workflow_fn = f"/cwl/{cwl_workflow}_workflow.cwl"

    # orig_workflow_fn = urlparse(airflow_report["workflow_def"]["id"]).path
    workflow_fn = os.path.join(output_dir, os.path.basename(orig_workflow_fn))
    shutil.copy(orig_workflow_fn, workflow_fn)

    steps = airflow_report["workflow_def"].get("steps", [])
    tools = [step["run"] for step in steps]
    for tool in tools:
        tool_path = urlparse(tool).path
        dest = os.path.join(output_dir, os.path.basename(tool_path))
        shutil.copy(tool_path, dest)

    report = {
        "workflow": os.path.basename(workflow_fn),
        "tools": [os.path.basename(t) for t in tools],
        "params": params_fn,
        "outs": {},
    }

    dag_run = get_current_context()["dag_run"]
    task_predictions = dag_run.get_task_instance("processing")
    report["start_date"] = task_predictions.start_date
    report["end_date"] = task_predictions.end_date

    workflow_keys = ["workflow_def", "workflow_params"]
    for key, value in airflow_report.items():
        if key not in workflow_keys:
            value["location"] = os.path.basename(value["location"])
            report["outs"][key] = value

    with open(os.path.join(output_dir, metadata_fn), "w") as report_file:
        yaml.dump(report, report_file)

    with open(os.path.join(output_dir, params_fn), "w") as params:
        json.dump(airflow_report["workflow_params"], params)

    # @fixme set real dates
    start_date = task_predictions.start_date.isoformat()
    end_date = task_predictions.end_date.isoformat()
    json.dump(
        {
            "tumor": [start_date, end_date],
            "tissue": [start_date, end_date],
            "extract-tissue-low/tissue": [start_date, end_date],
        },
        open(os.path.join(output_dir, "dates.json"), "w"),
    )

    slide_basename = airflow_report["workflow_params"]["slide"]["path"]
    slide_path = os.path.join(STAGE_DIR, slide_basename)
    shutil.copy(slide_path, output_dir)
    secondary_files = os.path.splitext(slide_path)[0]
    dest_secondary_files = os.path.join(output_dir, os.path.basename(secondary_files))
    logger.info("copying %s to %s", secondary_files, dest_secondary_files)
    if not os.path.exists(dest_secondary_files):
        shutil.copytree(secondary_files, dest_secondary_files)

    return output_dir


@task
def generate_rocrate(input_dir: str):
    command = f"-v {input_dir}:{input_dir} prov_crate  -o {input_dir}/rocrate {input_dir}".split()
    docker_run(command)


def tumor_branch(prediction_label, prediction, slide_label, report_dir):

    register_to_omero = task(
        _register_prediction_to_omero,
        task_id=f"add_{prediction.value}.tiledb_to_omero",
    )(f"{prediction_label}.tiledb", False)

    omero_id = str(register_to_omero["omero_id"])
    register_to_promort = task(
        add_prediction_to_promort,
        task_id=f"add_{prediction.value}.tiledb_to_promort",
    )(
        prediction.value,
        slide_label,
        f"{prediction_label}.tiledb",
        omero_id,
        report_dir=report_dir,
        review_required=True,
    )

    convert_to_tiledb(prediction_label) >> register_to_omero >> register_to_promort


@task
def create_tissue_fragments(prediction_id, shapes_filename):

    command = [
        "-v",
        f"{PREDICTIONS_DIR}:{PREDICTIONS_DIR}",
        PROMORT_TOOLS_IMG,
        "importer.py",
        "--host",
        f"{PROMORT_CONNECTION.conn_type}://{PROMORT_CONNECTION.host}:{PROMORT_CONNECTION.port}",
        "--user",
        PROMORT_CONNECTION.login,
        "--passwd",
        PROMORT_CONNECTION.password,
        "--session-id",
        PROMORT_SESSION_ID,
        "tissue_fragments_importer",
        "--prediction-id",
        str(prediction_id),
        shapes_filename,
    ]
    docker_run(command, network=DOCKER_NETWORK)


@task
def tissue_segmentation(label, path) -> str:
    threshold = Variable.get("ROI_THRESHOLD")
    out = os.path.join(PREDICTIONS_DIR, f"{label}_shapes.json")

    command = [
        "-v",
        f"{PREDICTIONS_DIR}:{PREDICTIONS_DIR}",
        PROMORT_TOOLS_IMG,
        "mask_to_shapes.py",
        f"{PREDICTIONS_DIR}/{os.path.basename(path)}",
        "-t",
        str(threshold),
        "-o",
        out,
        "--scale-func",
        "shapely",
        "--simplify",
        "0.8",
    ]
    docker_run(command, DOCKER_NETWORK)
    return out


def tissue_branch(dataset_label, dataset_path, prediction_id):
    #  TODO add variable for threshold
    shapes_filename = tissue_segmentation(dataset_label, dataset_path)
    create_tissue_fragments(prediction_id, shapes_filename)
