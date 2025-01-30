from datetime import date
import json
import os
import requests
import pymysql
import sys
import csv
from dotenv import load_dotenv
load_dotenv()
def default_headers():
    return [("MEESHO-ISO-COUNTRY-CODE", bytes("IN", 'utf-8'))]

connSupply = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
cursorSupply = connSupply.cursor()

connSlaveSupply = pymysql.connect(host= os.getenv('TAXONOMY_SLAVE_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
cursorSlaveSupply = connSlaveSupply.cursor()

sscatId = 10382
print("started for sscatId: ",sscatId)
carrierWeight = 188
print("carrierWeight: ",carrierWeight)
id=27048828
hasnxt = True
while hasnxt:
    sql = "SELECT contentId,id FROM content_sub_sub_category_map WHERE subSubCategoryid=%s and  id >%s order by id limit 200"
    cursorSlaveSupply.execute(sql, (sscatId,id))
    result = cursorSlaveSupply.fetchall()
    if result is None:
        print(f"Record with ID {id} not found.")
        continue

    if len(result) == 0:
        hasnxt = False
        continue

    if len(result) > 1:
        for record in result:
            id = int(record[1])
            product_id = int(record[0])
            print("product_id: ",product_id)
            reverseWeight={"reverse_carrier_weight":carrierWeight}
            attribute=json.dumps(reverseWeight,separators=('/', ':'), ensure_ascii=False)
            print("attribute: ",str(attribute))
            sql = "UPDATE products SET attributes= \'{\"reverse_carrier_weight\":%s}\' WHERE id=%s"
            cursorSupply.execute(sql, (carrierWeight,product_id))
            connSupply.commit()
print("completed for sscatId: ",sscatId)