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

min=100000
max=200000

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
    # We will create Key array to pass to delete_objects function
    for f in files_in_folder:
        files_to_delete.append({"Key": f["Key"]})
    # This will delete all files in a folder
    response = s3_client.delete_objects(
        Bucket=bucketName, Delete={"Objects": files_to_delete}
    )
    print(response)

def postRequest(path):
    session = boto3.Session(profile_name='default')
    s3 = session.resource("s3")
    input_source = {'Bucket': bucketName,'Key' : path }
    new_path='tmp-'+path
    try:
        bucket = s3.Bucket(bucketName)
        #bucket.copy(input_source, new_path)
        print("done for "+ path)
        #s3.Object('supplier-prod-temp-files', path).delete()
    except Exception as e :
        print('ERROR occurred '+ str(path) +' '+ str(e))
    else:
        print('Success for '+ str(path))


async def get_data_asynchronous(bodies):
    with ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [
            loop.run_in_executor(
                executor,
                postRequest,
                (body) # Allows us to pass in multiple arguments to `fetch`
            )
            for body in bodies
        ]
        for response in await asyncio.gather(*tasks):
            pass



files = []
productIdAttributeMap = {}
for filename in os.listdir(INPUT_DIR):
    if(filename.startswith("mweb_upload")):
        files.append(filename)
count =0
productsIds=[]
for file in files:
    with open(INPUT_DIR+file, 'r') as csvfile:
        # creating a csv reader object
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        
        # extracting field names through first row
        fields = next(csvreader)
        counter =1
        paths = []
        # extracting each data row one by one
        for row in csvreader:
            counter = counter + 1
            images = json.loads(row[2])
            if(len(images['front_image_url'])>1): 
                paths.append(urlparse(images['front_image_url']).path[1:])
            if len(images['other_images'])>0:
                for image in images['other_images']:
                    if image is not None and len(image) > 1:
                        paths.append(urlparse(image).path[1:])
        if(len(paths) > 0):
            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(get_data_asynchronous(paths))
            loop.run_until_complete(future)



    