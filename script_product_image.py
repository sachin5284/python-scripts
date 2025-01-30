import csv
import json
import time
import requests
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

INPUT_DIR = "/Users/sachinkumar/Documents/Testing/"

files = []
productIdAttributeMap = {}
for filename in os.listdir(INPUT_DIR):
    if(filename.startswith("sscats.cs")):
        files.append(filename)
count =0
productsIds=[]
for file in files:
    with open(INPUT_DIR+file, 'r') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        
        
        # extracting field names through first row
        fields = next(csvreader)
        
        # extracting each data row one by one
        rowIds=[]
        for row in csvreader:
            sscat=row[0]
            scaleIdNew=row[2]
            URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/sub-sub-category/scale/get/"+ str(sscat)
            HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
            r = requests.get(url = URL, headers = HEADERS)

            response = json.loads(r.text)
            request = {"data":[{"sub_sub_category_id":int(sscat), "scale_id":int(scaleIdNew)}]}
            URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/sub-sub-category/scale/bulk-create"
            r = requests.post(url = URL, data = json.dumps(request), headers = HEADERS)
            if r.status_code!=200:
                print("failed for sscat"+ str(sscat))
                continue

            if len(response['scale_ids']) > 0:
                print(sscat)
                for scaleId in response['scale_ids']:
                    if scaleId==int(scaleIdNew):
                        continue
                    print(scaleId)
                    request = {"data":[{"sub_sub_category_id":int(sscat), "scale_id":int(scaleId), "trashed":True}]}
                    URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/sub-sub-category/scale/bulk-create"
                    r = requests.post(url = URL, data = json.dumps(request), headers = HEADERS)
                    print(r.status_code)

            


            


        