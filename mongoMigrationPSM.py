import pymysql
import requests
import os
import json
import datetime
from dotenv import load_dotenv
load_dotenv()

# Connect to MySQL
mysql_conn = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
cursor = mysql_conn.cursor()
# Connect to MongoDBs

initialIds = 1321292089
finalIds = 1261939882
while(initialIds < finalIds) :
    sql = f"SELECT id,productId, supplierId, attributeName,attributeValue, rowLastUpdated FROM product_supplier_map_attributes where id > {initialIds} limit 100"
    cursor.execute(sql)
    rows = cursor.fetchall()
    attributesToBeUpdated=[]
    date = rows[0][5].timestamp()
    for row in rows:
        attributesToBeUpdated.append({
            'event':'product_supplier_map_attribute_event',
            'properties':{
                'product_id':int(row[1]),
                'supplier_id':int(row[2]),
                'attribute_name':row[3],
                'attribute_value':row[4],
                'time':int(row[5].timestamp()*1000)
            }
        }
        )
        initialIds=int(row[0])
    try:
        # Parse the JSON object from the line
        URL = 'http://prism-internal.meeshoint.in/api/v1/event/product_supplier_map_attribute_event'
        HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN",
                   "Authorization":'Basic NmFmZWJiNTczM2M4ZDdjMjIyMTM0MzE3ZTUwYzEwYjBiZGJkOmZmbjkzOWRlZGI2ODQ3MGZkc2FhZnFiNmIyNmY0NzE2ZDI0NzBlNmRkZA==',
                   "Content-Type":"application/json"}
        r = requests.post(url = URL, data = json.dumps(attributesToBeUpdated), headers = HEADERS)
        print(r.status_code)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {str(e)}")
    