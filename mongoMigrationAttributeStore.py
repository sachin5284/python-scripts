import pymysql
import pymongo
import os
import csv
import time
import datetime

# MySQL connection parameters

# MongoDB connection parameters
#mongo_uri = "mongodb://"+os.getenv('MONGO_USERNAME')+":"+ os.getenv('MONGO_PASSWORD')+"@"+os.getenv('MONGO_HOST')+"/taxonomy?authSource=admin"
mongo_uri = "mongodb://taxonomy_rw:mCn1t7Qr%40vCK1ZEC4A0DR7@mg-supl-ctlng-ty1.prd.meesho.int,mg-supl-ctlng-ty2.prd.meesho.int,mg-supl-ctlng-ty3.prd.meesho.int/taxonomy?authSource=admin&replicaSet=rs-taxonomy-mongo&readPreference=primaryPreferred&maxStalenessSeconds=300"
mongo_db = "taxonomy"
mongo_collection = "attribute_store"




# Connect to MySQL
mysql_conn = pymysql.connect(host= 'msql-taxonomy-data-slave.prd.meesho.int', port=3306, user='taxonomy', password='UQAufOdlT8Gf0r33OAmHg', database='taxonomy')
cursor = mysql_conn.cursor()
# Connect to MongoDB
mongo_client = pymongo.MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db]
mongo_coll = mongo_db[mongo_collection] 


contentIds=[]
with open('contentIds.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
        
        # extracting field names through first row
    fields = next(csvreader)

    # extracting each data row one by one
    for row in csvreader:
    #contentIds= [row[0]]
        productId=int(row[0])
        if productId in contentIds:
            continue
        contentIds.append(productId)
        # Fetch data from MySQL
        #print(productId)

        start = time.process_time()
        sql = f"SELECT contentId,`subSubCategoryId` FROM content_sub_sub_category_map where contentId={productId}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        contentMap={}
        for row in rows:
            contentMap[int(row[0])]=row
            #print(row)
        print(time.process_time() - start)
        start = time.process_time()
        query = {"contentId": productId}
        documents = mongo_coll.find(query)
        mongoMap={}
        for document in documents:
            mongoMap[(document['contentId'])]=document
        print(time.process_time() - start)
        start = time.process_time()
        mongoRecordsInsert=[]
        mongoRecordsUpdate=[]
        for key in contentMap:
            if key in mongoMap:
                continue
                updateRecord={"_id": mongoMap[key]['_id'], "subSubCategoryId": contentMap[key][1]}
                mongoRecordsUpdate.append(updateRecord)
            else:
                print(key)
                mongoRecord={"contentId": int(key), 
                                "attributes": {}, 
                                "subSubCategoryId": contentMap[key][1],
                                "createdAt": datetime.datetime.now(), 
                                "rowLastUpdated": datetime.datetime.now(), 
                                "_class": "com.meesho.citadel.dao.mongo.AttributeStoreMongo"}
                mongoRecordsInsert.append(mongoRecord)
        if(len(mongoRecordsInsert)>0):
            print(len(mongoRecordsInsert))
            mongo_coll.insert_many(mongoRecordsInsert)
        if(len(mongoRecordsUpdate)>0):
            update_operations = [
            pymongo.UpdateOne({"_id": doc["_id"]}, {"$set": {"subSubCategoryId":doc["subSubCategoryId"]}}, upsert=True)
            for doc in mongoRecordsUpdate
            ]
            result = mongo_coll.bulk_write(update_operations, ordered=True)
            print(time.process_time() - start)
        #contentIds=[]
                # Close database connections
mysql_conn.close()
mongo_client.close()