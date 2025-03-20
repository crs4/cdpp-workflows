#!/usr/bin/env python

# Copyright (c) 2021-2023 CRS4
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""\
Generate an RO-Crate for a workflow run.
"""

import argparse
import atexit
import json
import shutil
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from cwl_utils.parser import load_document_by_uri
from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


METADATA_BASENAME = "metadata.yaml"
WORKFLOW_NAME = "Promort tissue and tumor prediction"
WORKFLOW_URL = "https://github.com/crs4/deephealth-pipelines"
WORKFLOW_LICENSE = "MIT"
TYPE_MAP = {
    "string": "Text",
    "int": "Integer",
    "long": "Integer",
    "float": "Float",
    "double": "Float",
    "Any": "Thing",
    "boolean": "Boolean",
    "File": "File",
    "Directory": "Dataset",
}
MIRAX_URL = "https://openslide.org/formats/mirax/"
ZARR_URL = "https://zarr.readthedocs.io/en/stable/spec/v2.html"
PROFILES_BASE = "https://w3id.org/ro/wfrun"
PROFILES_VERSION = "0.5"
WROC_PROFILE_VERSION = "1.0"


def get_metadata(source):
    metadata_path = source / METADATA_BASENAME
    with open(metadata_path) as f:
        return yaml.load(f, Loader=Loader)


def get_params(source, metadata):
    params_path = source / metadata["params"]
    with open(params_path) as f:
        params = json.load(f)
    return params


def get_workflow(source, metadata):
    workflow_path = source / metadata["workflow"]
    return load_document_by_uri(workflow_path, load_all=True)


def get_param_types(params, wf_def):
    rval = {}
    inputs_by_id = {_.id.rsplit("#", 1)[1]: _ for _ in wf_def.inputs}
    for k, v in params.items():
        in_ = inputs_by_id[k]
        t = in_.type_
        if isinstance(t, list):
            t = [_ for _ in t if _ != "null"][0]
        rval[k] = TYPE_MAP[t]
    return rval


def add_profiles(crate):
    profiles = []
    for p in "process", "workflow":
        id_ = f"{PROFILES_BASE}/{p}/{PROFILES_VERSION}"
        profiles.append(crate.add(ContextEntity(crate, id_, properties={
            "@type": "CreativeWork",
            "name": f"{p.title()} Run Crate",
            "version": PROFILES_VERSION,
        })))
    wroc_profile_id = f"https://w3id.org/workflowhub/workflow-ro-crate/{WROC_PROFILE_VERSION}"
    profiles.append(crate.add(ContextEntity(crate, wroc_profile_id, properties={
        "@type": "CreativeWork",
        "name": "Workflow RO-Crate",
        "version": WROC_PROFILE_VERSION,
    })))
    crate.root_dataset["conformsTo"] = profiles


def add_action(crate, metadata):
    start_date = metadata["start_date"].isoformat()
    end_date = metadata["end_date"].isoformat()
    workflow = crate.mainEntity
    properties = {
        "@type": "CreateAction",
        "name": f"Promort prediction run on {start_date}",
        "startTime": start_date,
        "endTime": end_date,
    }
    action = crate.add(ContextEntity(crate, properties=properties))
    action["instrument"] = workflow
    return action


def add_params(params, param_types, metadata, source, crate, action):
    workflow = crate.mainEntity
    inputs, outputs, objects, results = [], [], [], []
    for k, v in params.items():
        add_type = "Collection" if k == "slide" else param_types[k]
        in_ = crate.add(ContextEntity(crate, f"{workflow.id}#{k}", properties={
            "@type": "FormalParameter",
            "name": k,
            "additionalType": add_type,
        }))
        # does it make sense for non-file params to have an encodingFormat?
        if k == "slide":
            in_["encodingFormat"] = MIRAX_URL
        inputs.append(in_)
        if isinstance(v, dict) and v.get("class") == "File":
            mrxs_path = source / v["path"]
            add_files_path = source / mrxs_path.stem
            mrxs_file = crate.add_file(
                mrxs_path,
                properties={"encodingFormat": MIRAX_URL}
            )
            add_files_dataset = crate.add_dataset(add_files_path)
            obj = crate.add(ContextEntity(crate, properties={
                "@type": "Collection"
            }))
            obj["mainEntity"] = mrxs_file
            obj["hasPart"] = [mrxs_file, add_files_dataset]
            crate.root_dataset.append_to("mentions", obj)
        else:
            obj = crate.add(ContextEntity(crate, f"#pv-{k}", properties={
                "@type": "PropertyValue",
                "name": k,
                "value": str(v),
            }))
        obj["exampleOfWork"] = in_
        objects.append(obj)
    workflow["input"] = inputs
    action["object"] = objects
    for k, v in metadata["outs"].items():
        assert v["class"] == "File"
        assert k not in params  # so that IDs are unique
        out = crate.add(ContextEntity(crate, f"{workflow.id}#{k}", properties={
            "@type": "FormalParameter",
            "name": k,
            "additionalType": "ImageObject",
            "encodingFormat": ZARR_URL,
        }))
        outputs.append(out)
        path = source / v["location"]
        assert path.is_file()
        res = crate.add_file(
            path,
            v["location"],
            fetch_remote=False,
            validate_url=False,
            properties={"encodingFormat": ZARR_URL, "contentSize": v["size"]}
        )
        res["exampleOfWork"] = out
        results.append(res)
    workflow["output"] = outputs
    action["result"] = results


def make_crate(source, out_dir):
    metadata = get_metadata(source)
    workflow_path = source / metadata["workflow"]
    crate = ROCrate(gen_preview=False)
    add_profiles(crate)
    wf_def = get_workflow(source, metadata)
    workflow = crate.add_workflow(
        workflow_path, metadata["workflow"], main=True, lang="cwl",
        lang_version=wf_def.cwlVersion, gen_cwl=False
    )
    workflow["name"] = crate.root_dataset["name"] = WORKFLOW_NAME
    crate.root_dataset["description"] = WORKFLOW_NAME
    workflow["url"] = crate.root_dataset["isBasedOn"] = WORKFLOW_URL
    crate.root_dataset["license"] = WORKFLOW_LICENSE
    # No README.md for now
    action = add_action(crate, metadata)
    params = get_params(source, metadata)
    param_types = get_param_types(params, wf_def)
    add_params(params, param_types, metadata, source, crate, action)
    crate.root_dataset.append_to("mentions", action)
    crate.write(out_dir)
    for step in wf_def.steps:
        tool_path = urlsplit(step.run).path
        shutil.copy(tool_path, out_dir)


def main(args):
    if zipfile.is_zipfile(args.run_report):
        source = tempfile.mkdtemp(prefix="gen_crate_")
        atexit.register(shutil.rmtree, source)
        with zipfile.ZipFile(args.run_report, "r") as zf:
            zf.extractall(source)
        source = Path(source)
    else:
        source = Path(args.run_report)
        if not source.is_dir():
            raise RuntimeError(
                "input must be either a zip file or a directory"
            )
    make_crate(source, args.out_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("run_report", metavar="RUN_REPORT",
                        help="workflow run report dir or zip")
    parser.add_argument("-o", "--out-dir", metavar="DIR",
                        default="pipeline_run",
                        help="output RO-Crate directory")
    main(parser.parse_args())
