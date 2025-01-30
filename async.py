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

def fetch(session, body):

    URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/products/edit"
    HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
    r = requests.post(url = URL, data = json.dumps(body), headers = HEADERS)
    elapsed = default_timer() - START_TIME
    time_completed_at = "{:5.2f}s".format(elapsed)
    print("{0:<30} {1:>20}".format(body['product_id'], time_completed_at))
    logging.info(r.content)
    return r
       

async def get_data_asynchronous(bodies):
    print("{0:<30} {1:>20}".format("File", "Completed at"))
    with ThreadPoolExecutor(max_workers=10) as executor:
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

def modifyAndValidate(url):
    if len(url.replace('_512','')) < 5:
        return url;
    return url.replace('_512','')
    

#connSupply = pymysql.connect(host='supply-prod-heevo-slave-01.cjdqi0zr8ovs.ap-southeast-1.rds.amazonaws.com', port=3306, user='bulletin_read', password='3Z8htw9XTV98eSEP', database='supply')
connSupply = pymysql.connect(host=os.getenv('SUPPLY_RDS'), port=3306, user=os.getenv('SUPPLY_USER'), password=os.getenv('SUPPLY_PASSWORD'), database='supply')

offset = 0
limit =100
resultT = lambda l: [item for sublist in l for item in sublist]
results = ()
productIds =[]
#147373
#42294800
productId=1000000
while (productId<2000000):
    querySupply = 'select id,images,catalogId from products where id > {} order by id ASC limit {} '.format(productId,limit)
    offset = offset + limit
    cursorSupply = connSupply.cursor()
    cursorSupply.execute(querySupply)
    resultSupply = cursorSupply.fetchall()
    imageUrl1= None;
    imageUrl2=None;
    imageUrl3=None;
    imageUrl4=None;
    bodies =[]
    for result in resultSupply:
        productId =result[0]
        if "_512" in  result[1]:
            logging.info(productId)
            productIds.append(productId)
            imageUrl1= None;
            imageUrl2=None;
            imageUrl3=None;
            imageUrl4=None;
            images =  result[1].split(',')
            for image in images:

                if(imageUrl1 is None):
                    imageUrl1 = modifyAndValidate(image)
                    continue;
                elif(imageUrl2 is None):
                    imageUrl2 = modifyAndValidate(image)
                    continue;
                elif(imageUrl3 is None):
                    imageUrl3 = modifyAndValidate(image)
                    continue;
                elif(imageUrl4 is None):
                    imageUrl4 = modifyAndValidate(image)
                    continue;
                else :
                    break;
            if(imageUrl1 is not None):
                request = {"product_id":result[0],"catalog_id":result[2], "image_url_1":imageUrl1,"image_edit_1":False}
                if(imageUrl2 is not None):
                    request["image_url_2"]=imageUrl2
                    request['image_edit_2']=False
                if(imageUrl3 is not None):
                    request["image_url_3"]=imageUrl3
                    request['image_edit_3']=False
                if(imageUrl4 is not None):
                    request["image_url_4"]=imageUrl4
                    request['image_edit_4']=False
                bodies.append(request)
    if len(bodies)>0:
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(get_data_asynchronous(bodies))
        loop.run_until_complete(future)