#!/usr/bin/env python
# -*- coding: utf-8 -*-
from cwl_airflow.extensions.cwldag import CWLDAG

dag = CWLDAG(workflow="/cwl/pca_classification_workflow.cwl",
             dag_id="pca_classification",
             concurrency=1)
