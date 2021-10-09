from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from pymongo import MongoClient
import boto3

import json
from datetime import datetime, timedelta
from io import BytesIO
from os import getenv


LANDING_ZONE = getenv('LANDING_ZONE', 'landing')
MONGO_URI = getenv('MONGO_URI')
DATE_FORMAT_STR = "%Y-%m-%d"
DATETIME_FORMAT_STR = "%Y-%m-%d %H:%M:%S"

@task
def start_pnadc_ingestion():
    print(f"[{datetime.now().strftime(DATETIME_FORMAT_STR)}] Started mongo_ingestion")

@task
def end_pnadc_ingestion():
    print(f"[{datetime.now().strftime(DATETIME_FORMAT_STR)}] Ended mongo_ingestion")

@task.python
def extract_and_load():
    ## [START mongo collection extraction]
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client.ibge
    pnadc = db.pnadc20203

    data = list(pnadc.find({}))
    ## [END mongo collection extraction]

    ## [START upload file into raw zone]
    now = datetime.now()
    filename = f"pnadc.json"
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
        f"pnadc/extract_date={now.strftime(DATE_FORMAT_STR)}/{filename}"
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
    schedule_interval="0 23 * * *",
    start_date=days_ago(1),
    tags = ["mongo", "ingestion", "igti"]
)
def pnadc_ingestion():
    start = start_pnadc_ingestion()
    extract = extract_and_load()
    end  = end_pnadc_ingestion()

    start >> extract >> end

dag = pnadc_ingestion()
