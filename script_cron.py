from pymongo import MongoClient, UpdateOne

# MongoDB connection
mongo_uri = "mongodb://taxonomy_rw:mCn1t7Qr%40vCK1ZEC4A0DR7@mg-supl-ctlng-ty1.prd.meesho.int,mg-supl-ctlng-ty2.prd.meesho.int,mg-supl-ctlng-ty3.prd.meesho.int/taxonomy?authSource=admin&replicaSet=rs-taxonomy-mongo&readPreference=primaryPreferred&maxStalenessSeconds=300"
mongo_db = "taxonomy"
mongo_collection = "conditional_visibility"

# Connect to MongoDB
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db]
collection = mongo_db[mongo_collection]  # Replace with your collection name

batch_size = 100
cursor = collection.find({}, {"_id": 1, "createdDate": 1,"rowLastUpdated":1}).batch_size(batch_size)
count=0
updates = []
for document in cursor:
    count=count+1
    document_id = document['_id']
    created_date = document.get('createdDate', None)
    update_date = document.get('rowLastUpdated', None)

    if update_date:
        print(document_id)
        continue

    if created_date:
        updates.append(
            UpdateOne(
                {"_id": document_id},
                {"$set": {"rowLastUpdated": created_date}}
            )
        )

    if len(updates) >= batch_size:
        print(count)
        collection.bulk_write(updates)
        updates = []

if updates:
    collection.bulk_write(updates)

print("Field 'rowLastUpdated' added to all documents.")
