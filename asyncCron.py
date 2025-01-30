from random import randint
import requests
import asyncio
import json
import logging
import pymysql
from concurrent.futures import ThreadPoolExecutor
from timeit import default_timer
import logging
import os
from dotenv import load_dotenv

load_dotenv()
    # Configure the logging system

logging.basicConfig(filename ='app.log',
                        level = logging.INFO)
START_TIME = default_timer()

def fetch(session, productId):
    body = {'product_id':productId,'image_url_1':'https://images.meesho.com/images/products/30278206/e70ca_512.jpg','image_edit_1':False}
    URL = os.getenv('TAXONOMY_ENDPOINT') +"/api/v1/products/edit"
    HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
    r = requests.post(url = URL, data = json.dumps(body), headers = HEADERS)
    print(r.status_code)
    print(productId)
       

async def get_data_asynchronous(bodies):
    print("{0:<30} {1:>20}".format("File", "Completed at"))
    with ThreadPoolExecutor(max_workers=4) as executor:
        with requests.Session() as session:
            # Set any session parameters here before calling `fetch`
            loop = asyncio.get_event_loop()
            START_TIME = default_timer()
            tasks = [
                loop.run_in_executor(
                    executor,
                    fetch,
                    *(session, body) # Allows us to pass in multiple arguments to `fetch`
                )
                for body in bodies
            ]
            for response in await asyncio.gather(*tasks):
                pass
    
loop = asyncio.get_event_loop()
productIds =[]
for i in range(1,1000):
    productIds.append(randint(100,390263))

loop.run_until_complete(get_data_asynchronous(productIds))
loop.close()