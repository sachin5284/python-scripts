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
    URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/internal/catalogs/collages/all/refresh/bulk"
    HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
    r = requests.post(url = URL, data = json.dumps(body), headers = HEADERS)
    elapsed = default_timer() - START_TIME
    time_completed_at = "{:5.2f}s".format(elapsed)
    logging.info('%s - %d',r.content,r.status_code)
    return r
       

async def get_data_asynchronous(bodies):
    print("{0:<30} {1:>20}".format("File", "Completed at"))
    with ThreadPoolExecutor(max_workers=3) as executor:
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

    

recordThreshold =1

#42294800

bodies =[]
fileList = ['catalogIds.csv']
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
            counter = counter + 1
            catalogId= row[0]
            request = {"data":[int(catalogId)]}
            bodies.append(request)
            if counter ==recordThreshold:
                counter = 0
                loop = asyncio.get_event_loop()
                future = asyncio.ensure_future(get_data_asynchronous(bodies))
                loop.run_until_complete(future)
                logging.info('done for batch')
                bodies=[]
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(get_data_asynchronous(bodies))
        loop.run_until_complete(future)