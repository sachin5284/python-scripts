import json
from kafka import KafkaProducer
import pymysql
import os
from dotenv import load_dotenv
load_dotenv()
connSupply = pymysql.connect(host=os.getenv('SUPPLY_RDS'), port=3306, user=os.getenv('SUPPLY_USER'), password=os.getenv('SUPPLY_PASSWORD'), database='supply')
cursorSupply = connSupply.cursor()

def default_headers():
    return [("MEESHO-ISO-COUNTRY-CODE", bytes("IN", 'utf-8'))]

def main():
    bootstrap_servers = os.getenv('KAFKA_BROKER')
    topic = "ct_qct_v2.catalog_activation"
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    file_path = "app.log"

    with open(file_path) as files:
        file = files.readlines()
        for line in file:
            request = json.loads(line)
            catalogId = request['catalog_id']

            queryProducts = "SELECT * from status_change_logs WHERE dataId={} AND type='catalog_valid_flag'".format(catalogId)
            cursorSupply.execute(queryProducts)
            resultSet = cursorSupply.fetchall()
            catalogAlreadyActivated = False
            for row in resultSet:
                if row[4] == 1:
                    print("Catalog already activated", catalogId)
                    catalogAlreadyActivated=True
                    break

            if catalogAlreadyActivated:
                continue

            print("Deleting status change log for catalog Id", catalogId)
            queryProducts = "DELETE from status_change_logs WHERE dataId={} AND type='catalog_valid_flag'".format(catalogId)
            cursorSupply.execute(queryProducts)
            connSupply.commit()
            print("Deletion successful of status change log for catalog Id", catalogId)

            print("Activating catalog Id", catalogId)
            producer.send(
                topic,
                value=request,
                headers=default_headers())
            print("Activation successful of catalog Id", catalogId)
main()