#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from utils import (
    Prediction,
    add_prediction_to_omero,
    add_prediction_to_promort,
    add_slide_to_omero,
    add_slide_to_promort,
    gather_report,
    generate_rocrate,
    handle_error,
    prepare_data,
    processing,
    tissue_branch,
    tumor_branch,
)

logger = logging.getLogger()
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "start_date": datetime(2019, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "on_failure_callback": handle_error,
}


def create_dag():
    with DAG(
        "pca_pipeline",
        on_failure_callback=handle_error,
        schedule_interval=None,
        max_active_runs=1,
        default_args=default_args,
    ) as dag:

        with TaskGroup(group_id="add_slide_to_backend"):
            slide = prepare_data()
            slide_info_ = add_slide_to_omero(slide)
            slide = slide_info_["slide"]
            slide_to_promort = add_slide_to_promort(slide_info_)

        dag_id = "pca_classification"
        dag_info = processing(dag_id)
        slide_to_promort >> dag_info
        report_dir = gather_report(dag_info, dag_id)
        generate_rocrate(report_dir)

        for prediction in Prediction:
            with TaskGroup(group_id=f"add_{prediction.value}_to_backend"):
                prediction_info = task(
                    add_prediction_to_omero, task_id=f"add_{prediction.value}_to_omero"
                )(prediction, dag_info)
                prediction_label = prediction_info["label"]
                prediction_path = prediction_info["path"]
                omero_id = str(prediction_info["omero_id"])

                prediction_id = task(
                    add_prediction_to_promort,
                    task_id=f"add_{prediction.value}_to_promort",
                )(prediction.value, slide, prediction_label, omero_id, report_dir)

                if prediction == Prediction.TUMOR:
                    tumor_branch(prediction_label, prediction, slide, report_dir)
                elif prediction == Prediction.TISSUE:
                    tissue_branch(prediction_label, prediction_path, prediction_id)
        return dag


dag = create_dag()
