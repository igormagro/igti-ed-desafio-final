from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from pymongo import MongoClient
import boto3

import json
from datetime import datetime, timedelta
from io import BytesIO


LANDING_ZONE = "imb-bronze-layer"
MONGO_URI = "mongodb+srv://estudante_igti:SRwkJTDz2nA28ME9@unicluster.ixhvw.mongodb.net/"
DATE_FORMAT_STR = "%Y-%m-%d"
DATETIME_FORMAT_STR = "%Y-%m-%d %H:%M:%S"

@task
def start_mongo_ingestion():
    print(f"[{datetime.now().strftime(DATETIME_FORMAT_STR)}] Started mongo_ingestion")

@task
def end_mongo_ingestion():
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
    filename = f"mongo_export.json"
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
        f"mongo/extract_date={now.strftime(DATE_FORMAT_STR)}/{filename}"
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
def mongo_ingestion():
    start = start_mongo_ingestion()
    extract = extract_and_load()
    end  = end_mongo_ingestion()

    start >> extract >> end

dag = mongo_ingestion()
