import pymysql
import csv
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()


connSupply = pymysql.connect(host= os.getenv('SUPPLY_RDS'), port=3306, user=os.getenv('SUPPLY_USER'), password=os.getenv('SUPPLY_PASSWORD'), database='supply')


URL = os.getenv('TAXONOMY_ENDPOINT') +"/api/v1/psm/update"
HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
    

#make it paginated
def updateProductSupplierMapForValidSuppliers(supplierId,valid):
    hasPsm = True
    limit = 50
    id = 0
    while(hasPsm):
        queryPsmValid = "select  productId, supplierId,id from product_supplier_map where supplierId = {} and validChangeType in (2,3) and valid=1 and id > {} order by id limit {}".format(supplierId,id, limit)
        cursorSupply = connSupply.cursor()
        cursorSupply.execute(queryPsmValid)
        resultPsm = cursorSupply.fetchall()
        if len(resultPsm) < limit:
            hasPsm= False

        if len(resultPsm) == 0:
            break
        payload =[]
        for result in resultPsm:
            id = result[2]
            if valid:
                temp = {"product_id":result[0],"supplier_id":result[1],"valid_change_type":1}
            else :
                temp = {"product_id":result[0],"supplier_id":result[1],"valid":0}
            payload.append(temp)
        request = {"payload":payload,"action_by":"ADMIN"}
        r = requests.post(url = URL, data = json.dumps(request), headers = HEADERS)
        data = r.content
        print(data)
    print('processed for supplierId'+ supplierId)


querySupply = 'select distinct(supplierId) from product_supplier_map where validChangeType in (2,3) and valid=1'
cursorSupply = connSupply.cursor()
cursorSupply.execute(querySupply)
resultSupply = cursorSupply.fetchall()
for result in resultSupply:
    supplierId = result[0]
    print(supplierId)
    querySupplier = 'select valid from suppliers where id = {}'.format(supplierId)
    cursorSupply = connSupply.cursor()
    cursorSupply.execute(querySupplier)
    resultSuppplier = cursorSupply.fetchall()
    if resultSuppplier[0][0] == 1:
        updateProductSupplierMapForValidSuppliers(supplierId,True)
    elif resultSuppplier[0][0] == 0:
        updateProductSupplierMapForValidSuppliers(supplierId,False)

