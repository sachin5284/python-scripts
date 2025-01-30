import pandas as pd
from pymongo import MongoClient

mongo_uri = "mongodb://taxonomy_rw:mCn1t7Qr%40vCK1ZEC4A0DR7@mg-supl-ctlng-ty1.prd.meesho.int,mg-supl-ctlng-ty2.prd.meesho.int,mg-supl-ctlng-ty3.prd.meesho.int/taxonomy?authSource=admin&replicaSet=rs-taxonomy-mongo&readPreference=primaryPreferred&maxStalenessSeconds=300"
mongo_db = "taxonomy"
mongo_collection = "conditional_visibility"

# Connect to MongoDB
client = MongoClient(mongo_uri)
mongo_db = client[mongo_db]
collection = mongo_db[mongo_collection] 

# Read entity_id from CSV file
def read_entity_ids_from_csv(file_path):
    # Assuming CSV has a column named 'entity_id'
    df = pd.read_csv(file_path)
    return df['entity_id'].tolist()

# Delete records from MongoDB in batches
def delete_records_by_entity_id(entity_ids, batch_size=100):
    total_records = len(entity_ids)
    for i in range(0, total_records, batch_size):
        batch = entity_ids[i:i + batch_size]
    
        # Query and delete records with entity_id in batch
        result = collection.delete_many({"entityId": {"$in": batch}})
        print(f"Batch {i//batch_size + 1}: Deleted {result.deleted_count} records.")

# Main function
def main():
    # Replace with your actual CSV file path
    csv_file_path = "entity_ids.csv"
    
    # Read entity_ids from the CSV file
    entity_ids = read_entity_ids_from_csv(csv_file_path)
    
    # Delete records in batches
    delete_records_by_entity_id(entity_ids, batch_size=100)

if __name__ == "__main__":
    main()
