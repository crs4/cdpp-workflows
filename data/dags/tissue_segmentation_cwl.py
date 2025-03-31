#!/usr/bin/env python
# -*- coding: utf-8 -*-
from cwl_airflow.extensions.cwldag import CWLDAG

dag = CWLDAG(workflow="/cwl/tissue_segmentation_workflow.cwl",
             dag_id="tissue_segmentation",
             concurrency=1)

