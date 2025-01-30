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

connSupply = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
cursorSupply = connSupply.cursor()

file_path = 'pidName.csv'
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file)
    
    # extracting field names through first row
    fields = next(csv_reader)
    for line in csv_reader:
        id=0
        productId = int(line[0])

        request = {"product_id":productId, 'name':line[1], 'update_product_images':False}
        URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/products/edit"
        HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
        r = requests.post(url = URL, data = json.dumps(request), headers = HEADERS)
        print(request)
        print(r.status_code)    
    