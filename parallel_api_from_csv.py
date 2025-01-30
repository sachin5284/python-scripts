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
    with ThreadPoolExecutor(max_workers=15) as executor:
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



bodies =[]
fileList = ['Failed_upload_new.csv']
for file in fileList:
    with open(file, 'r') as csvfile:
        print('starting for file '+ file)
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        
        # extracting field names through first row
        fields = next(csvreader)
        count =1
        counter=0
        bodies =[]
        # extracting each data row one by one
        for row in csvreader:
            productIdMap = json.loads(row[2])
            #print(productIdMap)
            
            try:
                attributes = json.loads(row[3])
            except:
                print(row[3])
                continue
            attributesRequest = []
            for attribute in attributes:
                attributesRequest.append({"name":attribute,"value":attributes[attribute]})
            subSubcategoryId = row[6]
            for productId in productIdMap:
                request = {"product_id":int(productId), "attributes":attributesRequest,"sub_sub_category_id":int(subSubcategoryId),"action_by":"db tagging script"}
                bodies.append(request)
            #loop = asyncio.get_event_loop()
            #future = asyncio.ensure_future(get_data_asynchronous(bodies))
            #loop.run_until_complete(future)
            #print(row)
            bodies=[]