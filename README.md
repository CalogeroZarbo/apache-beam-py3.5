# Dataflow (Apache Beam) with Python 3.5
In this little tutorial I will demo how to run a simple pipeline that read records from CSV files stored in 
Google Storage and upload them on Google BigQuery using Apache Beam (Google Dataflow) with Python3.5

## Installation

`git clone https://github.com/CalogeroZarbo/apache-beam-py3.5`

`cd apache-beam-py3.5`

`conda create -n apache-beam-py3.5 python=3.5`

`conda activate apache-beam-py3.5`

`pip install -r requirements.txt `

## Usage

In order to run this simple tutorial you would need:
1. Google Cloud Platform account
2. Google Cloud Storage folder where to put all the CSV files you would like to read
    1. Create a GS Bucket and call it `sample_bucket`
3. BigQuery project called `sample_project`
    1. BigQuery dataset under that project called `dataflow_tutorial`

The CSV table format (as per `dataflow_tutorial/bigquery_table_specs.py`) should look like:

| COL1 | COL2 | COL3 |
| ---- | ---- | ---- |
| val1 | val2 | val3 |

You can change the format, the name of the BigQuery project and the table name by modifying the file `dataflow_tutorial/bigquery_table_specs.py`.

`export PROJECT=sample_project`

`export WORK_DIR=gs://sample_bucket/sample_data/`

`python preprocess.py --project $PROJECT --runner DataflowRunner --temp_location $WORK_DIR/beam-temp --setup_file ./setup.py --work-dir $WORK_DIR --region europe-west1`

## Brief Explanation of the files

- `setup.py` will install the package with the pipeline to run in the Apache Beam distributed machines
- `preprocess.py` is the main file where the Apache Beam pipeline is defined
- `dataflow_tutorial/` is the folder with the pipeline files needed to run the preprocessing properly
- `bigquery_table_specs.py` contains the specifications for the tables on BigQuery
- `pipeline_utils.py` contains the classes to read the CSV files, and handle the different chunks in different machines
- `record_utils.py` containes the definitions of the processing steps to perform on the records that has been read from the CSV
