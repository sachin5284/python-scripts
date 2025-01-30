import pymongo
import csv

# MySQL connection parameters

# MongoDB connection parameters
#mongo_uri = "mongodb://"+os.getenv('MONGO_USERNAME')+":"+ os.getenv('MONGO_PASSWORD')+"@"+os.getenv('MONGO_HOST')+"/taxonomy?authSource=admin"
mongo_uri = "mongodb://taxonomy_rw:mCn1t7Qr%40vCK1ZEC4A0DR7@mg-supl-ctlng-ty1.prd.meesho.int,mg-supl-ctlng-ty2.prd.meesho.int,mg-supl-ctlng-ty3.prd.meesho.int/taxonomy?authSource=admin&replicaSet=rs-taxonomy-mongo&readPreference=primaryPreferred&maxStalenessSeconds=300"
mongo_db = "taxonomy"
mongo_collection = "conditional_visibility"

# Connect to MongoDB
mongo_client = pymongo.MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db]
mongo_coll = mongo_db[mongo_collection] 

cursor = mongo_coll.find()

# Iterate through the documents
for document in cursor:
    print(document)

with open('productScore.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    fields = next(csvreader)
    for row in csvreader:
        contentMap={}
        productId=int(row[1])
        rank=int(row[2])
        
        contentMap[int(row[1])]=rank

        query = {"contentId":  productId}
        documents = mongo_coll.find(query)
        mongoMap={}
        for document in documents:
            mongoMap[(document['contentId'])]=document

        mongoRecordsInsert=[]
        mongoRecordsUpdate=[]
        for key in contentMap:
            if key in mongoMap:
                updateRecord={"_id": mongoMap[key]['_id'], "rank": contentMap[key]}
                mongoRecordsUpdate.append(updateRecord)
            else:
                print(f"No record available for contentid {productId}")
        if(len(mongoRecordsInsert)>0):
            print(len(mongoRecordsInsert))
            mongo_coll.insert_many(mongoRecordsInsert)
        if(len(mongoRecordsUpdate)>0):
            print(productId)
            update_operations = [
            pymongo.UpdateOne({"_id": doc["_id"]}, {"$set": {"rank":doc["rank"]}}, upsert=True)
            for doc in mongoRecordsUpdate
            ]
            result = mongo_coll.bulk_write(update_operations,ordered=False)
        contentIds=[]
                # Close database connections
mongo_client.close()