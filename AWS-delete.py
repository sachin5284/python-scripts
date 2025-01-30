import boto3
import csv
import asyncio
import logging
import json
from concurrent.futures import ThreadPoolExecutor
from timeit import default_timer
from urllib.parse import urlparse
import os
from dotenv import load_dotenv

load_dotenv()


INPUT_DIR = "/Users/sachinkumar/Documents/Testing/"

logging.basicConfig(filename ='app.log',
                        level = logging.INFO)
START_TIME = default_timer()
bucketName = 'supplier-prod-temp-files'

session = boto3.Session(profile_name='prod')
s3_client = session.client("s3")
# First we list all files in folder
hasNext=True
while(hasNext):
    response = s3_client.list_objects_v2(Bucket=bucketName, Prefix="tmp-products-upload/")
    files_in_folder = response["Contents"]
    files_to_delete = []
    if len(files_in_folder)<10:
        hasNext=False
    # We will create Key array to pass to delete_objects function
    for f in files_in_folder:
        files_to_delete.append({"Key": f["Key"]})
    # This will delete all files in a folder
    response = s3_client.delete_objects(
        Bucket=bucketName, Delete={"Objects": files_to_delete}
    )
    print(response)