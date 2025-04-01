# Introduction
This repository contains the computational workflows for the [CRS4 Digital Pathology Platform](https://github.com/crs4/DigitalPathologyPlatform)(CDPP).
It leverages [CWL-Airflow](https://barski-lab.github.io/cwl-airflow/) for processing whole slide images in a scalable and reproducible way. Each run takes as input a slide and can produce one or more outputs. At the moment, two workflows are provided: a basic one, which segments tissue on H&E slides, and another one providing, in addition to the tissue segmentation, prostate cancer classification.
The outputs of segmentation and classification tasks can be loaded in the CDPP, to be visualized as vectorial shapes and heatmaps respectively.

## Workflow structure
![Basic Pipeline](./docs/basic_pipeline.png)

The picture above shows the structure of a basic workflow. Actually, it is the *basic_pipeline*  defined in ```data/dags/basic_pipeline.py```. The first step is ingesting the input slide into the CDPP. The second step is processing the slide. It can include multiple sub-steps; moreover, it can be defined in [CWL](https://www.commonwl.org/), to guarantee reproducibility. Next, the ouput (in this case the tissue segmentation) is loaded back into the CDPP. In parallel, provenance is tracked generating a [Workflow Run RO-Crate](https://www.researchobject.org/workflow-run-crate/). 


# Getting started
The preferred way to run workflows on the CDPP is via docker compose.
First, create, create a ```.env``` file executing:
```
./create_env.sh
```
Then edit the output ```.env```. Be sure to set safe user/password values.


Edit the env variable ```CWL_DOCKER_GPUS ``` for setting the gpus to be used on the docker container used for predictions.

N.B.
Change ```omeseadragon:4080``` to `ome_seadragon.base_url` and `ome_seadragon.static_files_url` to the local machine address.
The port is the same specified in omeseadragon-nginx service (docker-compose.omero.yaml).

## Deployment

```
./compose.sh up -d
```

Check if the ```init``` service exited with 0 code, otherwise restart it. It can fail for timing reason, typically because SQL tables do not exist yet.

To visit Airflow, go to `http://localhost:<AIRFLOW_WEBSERVER_PORT>`.



## Upload data

Once the services are up and running (via ```./compose.sh```), put some data in the INPUT_DIR (variable defined in your .env file). For testing purpose, as input you can use the slide ```tests/data/Mirax2-Fluorescence-2.mrxs``` 

You can use ```slide_importer/local.py``` for running the *basic_pipeline* , i.e. the slide ingestion and the tissue segmentation (for H&E WSIs), or the more complex *pca_pipeline*, which classifies prostate cancer in addition to the tissue segmentation.

```bash
cd slide-importer
poetry install
poetry run python slide_importer/local.py basic_pipeline  --user $AIRFLOW_USER -P $AIRFLOW_PASSWORD --server-url http://localhost:$AIRFLOW_WEBSERVER_PORT  --wait --params '{"level": 8}'
# or 
poetry run python slide_importer/local.py pca_pipeline --user $AIRFLOW_USER -P $AIRFLOW_PASSWORD --server-url http://localhost:$AIRFLOW_WEBSERVER_PORT -p '{ "tissue-high-level": 8, "tissue-high-filter": "tissue_low>1", "tumor-filter": "tissue_low>1", "gpu": null}'  --wait 
```

Parameters for the *basic_pipeline* are defined in ```cwl/extract_tissue.cwl```, while the ones for the *pca_pipeline* are defined in ```cwl/pca_classification_workflow.cwl```.

# Extending workflows
First, It is strongly suggested to read the documentation of [Apache Airflow](https://airflow.apache.org/). Consider also that the Python module ```data/dags/utils.py``` contains many useful functions.

The easiest way for developing a custom workflow is to copy the ```data/dags/basic_pipeline.py``` on another file, under the *dags* directory. It is suggested to create the custom processing as a *CWL* file, under the directory ```./cwl```. You have to create a Python companion file under the ```dags``` directory, see the ```_cwl.py``` files as an example. For executing the right *CWL*, you have to call the ```processing function``` (defined in ```data/dags/utils.py```) with the dag id defined in the Python companion file. 

For uploading output to the CDPP, in general you have to upload first to OMERO. Take a look at *add_prediction_to_omero* and *add_prediction_to_promort* functions.
For creating vectorial shapes, take a look at the *tissue_branch*  function. For making visual predictions as heatmaps, see *tumor_branch*. 
