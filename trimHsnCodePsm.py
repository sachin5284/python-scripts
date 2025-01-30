import csv
import json
from math import prod
import requests
import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

connSupply = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
cursorSupply = connSupply.cursor()

with open("productIds.csv", 'r') as csvfile:
    # creating a csv reader object
    csvreader = csv.reader(csvfile)
      
    # extracting field names through first row
    fields = next(csvreader)
    count =1
    # extracting each data row one by one
    for row in csvreader:
        productId= row[0]
        sql = "SELECT productId, supplierId, hsnCode FROM product_supplier_map WHERE productId =%s"
        cursorSupply.execute(sql, (productId))
        result = cursorSupply.fetchone()
        supplierId=result[1]
        hsnCode=result[2]
        if len(hsnCode)>6:
            hsnCode = hsnCode[:6]
            request = { "payload":[{"product_id":productId, "supplier_id":supplierId,"hsn_code":hsnCode}], 'action_by':"ADMIN"} 
            URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/psm/update"
            HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
            r = requests.post(url = URL, data = json.dumps(request), headers = HEADERS)
            print(request)
            print(r.status_code)
        