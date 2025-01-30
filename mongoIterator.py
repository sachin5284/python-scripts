from pymongo import MongoClient, UpdateOne

# MongoDB connection
mongo_uri = "mongodb://taxonomy_rw:mCn1t7Qr%40vCK1ZEC4A0DR7@mg-supl-ctlng-ty1.prd.meesho.int,mg-supl-ctlng-ty2.prd.meesho.int,mg-supl-ctlng-ty3.prd.meesho.int/taxonomy?authSource=admin&replicaSet=rs-taxonomy-mongo&readPreference=primaryPreferred&maxStalenessSeconds=300"
mongo_db = "taxonomy"
mongo_collection = "conditional_visibility"
nqdReasons=['High Seller NQD - unrated catalogs'.lower(),'High seller NQD - unscaled catalogs'.lower(),'High Seller NQD + High SSCAT NQD - unscaled catalogs'.lower()]    

reasonDescriptions =['This catalog is blocked due to poor quality score. Quality score = Number of 1 or 2 star ratings / total ratings on this catalog. Example : If a catalog has a total of 80 ratings, out of which 20 ratings are 1 or 2 star, then,  quality score = 20/80 = 25%. Catalog quality score should be below 15% for full visibility.',
                     'This catalog is reduced to limited visibility due to poor quality score on your account (Even if this catalog has no ratings or less number of ratings, the number of 1 or 2 star ratings on your other catalogs are high. (Poor quality score = Number of 1 or 2 star ratings / total ratings on all your catalogs. Example : If your account (all catalogs) have a total of 200 ratings, out of which 40 ratings are 1star or 2star, then quality score = 40/200 = 20%. Account quality score should be below 15% to have full visibility on catalogs.',
                     'This catalog is blocked due to High Wrong Fulfillment Rate by supplier. This means that wrong products/quantity have been dispatched repeatedly or product description does not match the dispatched product. (WFR = Number of wrong fulfilled orders/total orders on this catalog). Keep WFR below 5% on the rest of your catalogs to prevent them from being blocked.'
                     ]

    
    
    
# Connect to MongoDB
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db]
collection = mongo_db[mongo_collection]  # Replace with your collection name
def is_empty_or_null(s):
    # Check if the string is None or empty after trimming
    return s is None or s.strip() == ""

batch_size = 1000
cursor = collection.find({}, {"_id": 1, "reasonDescription": 1}).batch_size(batch_size)
count=0

updates = []
for doc in cursor:
    count += 1
    if count%10000==0:
        print(count)
    if doc['reasonDescription'] not in reasonDescriptions:
        print(doc['_id'])
