from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import requests
import boto3

import json
from datetime import datetime, timedelta
from io import BytesIO


LANDING_ZONE = "imb-bronze-layer"
IBGE_ENDPOINT = "https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes"
FILENAME = "ibge_mesoregions_export"
DATE_FORMAT_STR = "%Y-%m-%d"
DATETIME_FORMAT_STR = "%Y-%m-%d %H:%M:%S"

@task
def start_ibge_ingestion():
    print(f"[{datetime.now().strftime(DATETIME_FORMAT_STR)}] Started ibge_ingestion")

@task
def end_ibge_ingestion():
    print(f"[{datetime.now().strftime(DATETIME_FORMAT_STR)}] Ended ibge_ingestion")

@task.python
def extract_and_load():
    ## [START mongo collection extraction]
    res = requests.get(IBGE_ENDPOINT)

    data = res.json()
    ## [END mongo collection extraction]

    ## [START upload file into raw zone]
    now = datetime.now()
    filename = f"{FILENAME}.json"
    obj = BytesIO(
            json.dumps(
                data,
                default=str,
                indent=4,
                sort_keys=True,
                ensure_ascii=False
            ).encode("utf8")
        )
    
    s3_client = boto3.client("s3")
    s3_client.upload_fileobj(
        obj,
        LANDING_ZONE,
        f"ibge/mesoregions/extract_date={now.strftime(DATE_FORMAT_STR)}/{filename}"
    )
    ## [END upload file into raw zone]

default_args = {
    'owner': 'Igor Magro',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

@dag(
    default_args=default_args,
    schedule_interval="20 23 * * *",
    start_date=days_ago(1),
    tags = ["ibge", "ingestion", "igti"]
)
def ibge_mesoregions_ingestion():
    start = start_ibge_ingestion()
    extract = extract_and_load()
    end  = end_ibge_ingestion()

    start >> extract >> end

dag = ibge_mesoregions_ingestion()
