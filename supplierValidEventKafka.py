from datetime import date
import json
import uuid
from kafka import KafkaProducer
import os
import datetime
import itertools
import csv
from dotenv import load_dotenv
load_dotenv()
def default_headers():
    return [("MEESHO-ISO-COUNTRY-CODE", bytes("IN", 'utf-8'))]

bootstrap_servers = os.getenv('KAFKA_BROKER')
print(bootstrap_servers)
topic = "supplierstore.supplier.upsert"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))

file_path = 'validSuppliers.csv'
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file)
    
    # extracting field names through first row
    fields = next(csv_reader)
    for row in csv_reader:
        supplierId =int(row[0])
        data = {"supplier_id":supplierId,"op_type":"UPDATE","old_data":{"valid":False,"valid_change_type":2},"new_data":{"valid":True,"valid_change_type":1},"restore_products":True,"qc_update":False,"action_by":"ADMIN","moderator":"jaya.sahu_technotask@meesho.com"}
        meta = {"requestTimestamp":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"requestId":str(uuid.uuid1())}
        payload = {'meta':meta,'data':json.dumps(data)}
        print(payload)
        producer.send(topic,value=payload,headers=default_headers())