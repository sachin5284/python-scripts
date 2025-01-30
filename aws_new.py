import boto3
import csv
import asyncio
import requests
import logging
import json
from concurrent.futures import ThreadPoolExecutor
from timeit import default_timer
from urllib.parse import urlparse
import os
from dotenv import load_dotenv

load_dotenv()

min=0
max=100000

INPUT_DIR = "/home/ubuntu/"

logging.basicConfig(filename ='app.log',
                        level = logging.INFO)
START_TIME = default_timer()
bucketName = 'supplier-prod-temp-files'


def postRequest(bucket,paths):
    try:
        response = bucket.delete_objects(Delete={
                'Objects': [{
                    'Key': key
                } for key in paths]
            })
        if 'Deleted' in response:
            logging.info(
                "Deleted objects '%s' from bucket '%s'.",
                [del_obj['Key'] for del_obj in response['Deleted']], bucket.name)
        if 'Errors' in response:
            logging.warning(
                "Could not delete objects '%s' from bucket '%s'.", [
                    f"{del_obj['Key']}: {del_obj['Code']}"
                    for del_obj in response['Errors']],
                bucket.name)
    except Exception as e :
        print('ERROR occurred '+' '+ str(e))
    else:
        print('Success for '+ str(paths))






hasNext =True
iterator ='1637456262520-273757-10109-IN'
limit =100000
maxsize=1000000
counter=0
while(hasNext):
    if(counter >= maxsize):
        print('processed all records')
        print(iterator)
        break
    body={
         'file_ref_id':iterator,
            'size':100000
    }
    URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/internal/presto/fetch"
    HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
    r = requests.post(url = URL, data = json.dumps(body), headers = HEADERS)

    if r.status_code != 200:
        print(r.text + str(body))
        continue
        # extracting each data row one by one
    response_dict = json.loads(r.text)
    paths = []
    session = boto3.Session(profile_name='prod')
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucketName)
    for row in response_dict['items']:
        iterator=row['fileRefId']
        frontImageUrl=row['frontImageUrl']
        otherImage1=row['otherImage1']
        otherImage2=row['otherImage2']
        otherImage3=row['otherImage3']
        if len(frontImageUrl) > 0:
            paths.append(urlparse(frontImageUrl).path[1:])
        if otherImage1 is not None and len(otherImage1) > 0:
            paths.append(urlparse(otherImage1).path[1:])
        if otherImage2 is not None and len(otherImage2) > 0:
            paths.append(urlparse(otherImage2).path[1:])
        if otherImage3 is not None and len(otherImage3) > 0:
            paths.append(urlparse(otherImage3).path[1:])
        print(counter)
        counter = counter + 1
        if(counter % 100 == 0):
            postRequest(bucket=bucket,paths=paths)
            paths = []
            print("processing done for "+ str(row))

    if(len(paths) > 0):
        postRequest(bucket=bucket,paths=paths)

print("final processing done")