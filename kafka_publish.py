from datetime import date
import json
import uuid
from kafka import KafkaProducer
import os
import datetime
import pymysql
import itertools
import csv
from dotenv import load_dotenv
load_dotenv()
def default_headers():
    return [("MEESHO-ISO-COUNTRY-CODE", bytes("IN", 'utf-8'))]

connSupply = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
cursorSupply = connSupply.cursor()

def partition(l, size):
    for i in range(0, len(l), size):
        yield list(itertools.islice(l, i, i + size))

iterator = 0
hasNext=True
subSubCateogryId=10003
bootstrap_servers = os.getenv('KAFKA_BROKER')
print(bootstrap_servers)
topic = "taxonomy.catalog_update_cumulative_fieldsTe"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))

file_path = 'invalidCatalogs.csv'
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file)
    
    # extracting field names through first row
    fields = next(csv_reader)
    for row in csv_reader:
        catalogId =int(row[0])
        data = {'catalog_id':catalogId, "action_by":"ADMIN","activate_catalog":True}
        meta = {"requestTimestamp":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"requestId":str(uuid.uuid1())}
        payload = {'meta':meta,'data':json.dumps(data)}
        print(payload)
        producer.send(topic,value=payload,headers=default_headers())


    