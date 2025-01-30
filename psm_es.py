import json
import pymysql
import os
import csv
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError


load_dotenv()

def default_headers():
    return [("MEESHO-ISO-COUNTRY-CODE", bytes("IN", 'utf-8'))]


def main():
    connSupply = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
    cursorSupply = connSupply.cursor()
    bootstrap_servers = os.getenv('KAFKA_BROKER')
    topic = "catalog_search.psm.create_update"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))
   
    
    with open('product_with_multiple_supplier.csv', 'r') as csvfile:  
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        
        # extracting field names through first row
        fields = next(csvreader)
        count =1
        counter=0
        bodies =[]
        # extracting each data row one by one
        for row in csvreader:
            productId = json.loads(row[0])
            if int(row[1]) > 1:
                payload = {
                    'data': '{\"product_id\":' + str(productId) + '}'
                }
                print(payload)
                i += 1
                future = producer.send(
                    topic,
                    value=payload,
                    headers=default_headers())
                try:
                    future.get(timeout=10)
                except KafkaError:
                    print(KafkaError)

main()
