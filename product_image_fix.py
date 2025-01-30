import csv
import pymysql
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

connSupply = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
cursorSupply = connSupply.cursor()

lastId = 0
hasNext=True

fileList = ['productSorting.csv']
csvInput =[]
supplierids=[]
for file in fileList:
    with open(file, 'r') as csvfile:
        print('starting for file '+ file)
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        
        # extracting field names through first row
        fields = next(csvreader)
        count =1
        counter=0
        bodies =[]
        # extracting each data row one by one
        for row in csvreader:
            print(row)
            productId=int(row[0])
            sortRank=int(row[4])
            audienceId=14
            countryCode ='IN'
            #print(supplierId)
            sql = f"INSERT INTO product_sorting  (productId, audienceId, `rank`, isoCountryCode) values ({productId}, {audienceId}, {sortRank},'{countryCode}' ) ON DUPLICATE KEY UPDATE `rank` = {sortRank}"
            #sql = "Update insert into product_sorting (product_id, sort_rank, audience_id, attribute_name) values (%s, %s, %s, %s)"
            cursorSupply.execute(sql)
            connSupply.commit()
        