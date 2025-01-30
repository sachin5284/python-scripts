from datetime import date
import json
import os
import requests
import pymysql
import csv
from dotenv import load_dotenv
load_dotenv()
def default_headers():
    return [("MEESHO-ISO-COUNTRY-CODE", bytes("IN", 'utf-8'))]

taxonomyConnection = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
taxonomyCursor = taxonomyConnection.cursor()
taxonomySlaveConnection = pymysql.connect(host= os.getenv('TAXONOMY_SLAVE_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
taxonomySlaveCursor = taxonomySlaveConnection.cursor()

def partition(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i+size]

file_path = 'sscatWeight.csv'
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file)
    
    # extracting field names through first row
    fields = next(csv_reader)
    for row in csv_reader:
        sscatId = int(row[0])
        print("started for sscatId: ",sscatId)
        carrierWeight = int(float(row[1]))
        print("carrierWeight: ",carrierWeight)
        id=0
        hasnxt = True
        while hasnxt:
            sql = "SELECT contentId,id FROM content_sub_sub_category_map WHERE subSubCategoryid=%s and  id >%s order by id limit 200"
            taxonomySlaveCursor.execute(sql, (sscatId,id))
            result = taxonomySlaveCursor.fetchall()
            if result is None:
                print(f"Record with ID {id} not found.")
                continue

            if len(result) == 0:
                hasnxt = False
                continue

            if len(result) > 0:
                productIds=[]
                lastProductId=0
                for record in result: 
                    id = int(record[1])
                    lastProductId=record[0]
                    productIds.append(str(record[0]))
                print("product_id: ",lastProductId)
                productList = ",".join(productIds)
                reverseWeight={"reverse_carrier_weight":carrierWeight}
                attribute=json.dumps(reverseWeight,separators=('/', ':'), ensure_ascii=False)
                print("attribute: ",str(attribute))
                sql = "UPDATE products SET attributes= \'{\"reverse_carrier_weight\":%s}\' WHERE id in "+ f"({productList})"""
                taxonomyCursor.execute(sql, (carrierWeight))
                taxonomyCursor.commit()
        print("completed for sscatId: ",sscatId)