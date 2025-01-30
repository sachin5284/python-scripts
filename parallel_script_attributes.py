import requests
import asyncio
import json
import logging
import pymysql
from concurrent.futures import ThreadPoolExecutor
from timeit import default_timer
import logging
import os
import csv
from dotenv import load_dotenv

load_dotenv()

    # Configure the logging system

logging.basicConfig(filename ='app.log',
                        level = logging.INFO)
START_TIME = default_timer()

def postRequest(session, body):
    URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/product/attributes/partial-update"
    HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
    r = requests.post(url = URL, data = json.dumps(body), headers = HEADERS)
    if r.status_code != 200:
        print(r.text + str(body))
    return r
       

async def get_data_asynchronous(bodies):
    with ThreadPoolExecutor(max_workers=10) as executor:
        with requests.Session() as session:
            # Set any session parameters here before calling `fetch`
            loop = asyncio.get_event_loop()
            START_TIME = default_timer()
            tasks = [
                loop.run_in_executor(
                    executor,
                    postRequest,
                    *(session, body) # Allows us to pass in multiple arguments to `fetch`
                )
                for body in bodies
            ]
            for response in await asyncio.gather(*tasks):
                pass


INPUT_DIR = "/Users/sachinkumar/Documents/Testing/"

files = []
productIdAttributeMap = {}
for filename in os.listdir(INPUT_DIR):
    if(filename.startswith("ds_tagging_new")):
        files.append(filename)
count =0
productsIds=[]
for file in files:
    with open(INPUT_DIR+file, 'r') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        
        # extracting field names through first row
        fields = next(csvreader)
        
        # extracting each data row one by one
        rowIds=[]
        for row in csvreader:
            productIdAttributeMap.setdefault(int(row[3]), []).append({"name":row[4],"value":row[5]})
            count = count + 1
print(count)

counter=0
bodies =[]
for productId in productIdAttributeMap:
    request = {"product_id":int(productId), "attributes":productIdAttributeMap[productId],"source":"db tagging script"}
    bodies.append(request)
    counter = counter + 1
    print(productId)
    if(counter % 200 == 0):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(get_data_asynchronous(bodies))
        loop.run_until_complete(future)
        bodies=[]
if(len(bodies) > 0):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(bodies))
    loop.run_until_complete(future)
    bodies=[]