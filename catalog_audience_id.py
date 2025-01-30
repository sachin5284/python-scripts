import pymysql
import csv
import requests
import json
from datetime import datetime
import os
import time
from dotenv import load_dotenv

load_dotenv()



connSupply = pymysql.connect(host= os.getenv('SUPPLY_RDS_MASTER'), port=3306, user=os.getenv('SUPPLY_USER'), password=os.getenv('SUPPLY_PASSWORD'), database='supply',autocommit=True)
cursorSupply = connSupply.cursor()
def myFunc(e):
  return int(e)

with open("catalogs.csv", 'r') as csvfile:
    # creating a csv reader object
    csvreader = csv.reader(csvfile,delimiter=' ', quotechar='|')
      
    # extracting field names through first row
    fields = next(csvreader)
    count =1
    catalogIds = []
    # extracting each data row one by one
    for row in csvreader:
        count=count+1
        if count <549000:
            continue
        catalogIds.append(row[0])
    catalogIds.sort(key=myFunc)
    queryProducts = "update catalogs set audienceId =1 WHERE id IN ({})".format(",".join(catalogIds))
    cursorSupply.execute(queryProducts)
    catalogIds = []
    print(row)
    time.sleep(0.5)
