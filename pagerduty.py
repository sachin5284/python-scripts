import requests
import csv
from time import sleep

def bulk_upsert_catalog(catalog_ids):
    url = 'http://supplier-search.prd.meesho.int/api/v1/catalog-management/supplier/catalog/bulk-upsert'
    headers = {
        'Content-Type': 'application/json',
        'Authentication': 'kLSlxSTbsrrlK45GXvmGRGYK4QYiX6lpL6y9LQoSBkbH5qjC'
    }
    payload = []
    for catalogId in catalog_ids:
        payload.append({"catalog_id": catalogId})
    data = {"payload": payload}
    print(data)
    try:
        response = requests.post(url, headers=headers, json=data)
        print(f"Status Code: {response.status_code}")
        return response.status_code, response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None, None

def read_catalog_ids_from_csv_files(file_path):
    catalog_ids = []
    print(f"Reading file: {file_path}")
    try:
        with open(file_path, mode='r', newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                if 'id' in row:  # Assumes 'id' is the column name for catalog IDs
                    catalog_ids.append(row['id'])
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    
    return catalog_ids

def process_catalogs_from_csv(file_list, batch_size=10):
    for file_path in file_list:
        catalog_ids = read_catalog_ids_from_csv_files(file_path)
        # Process the catalog IDs in batches
        for i in range(0, len(catalog_ids), batch_size):
            batch = catalog_ids[i:i + batch_size]
            bulk_upsert_catalog(batch)
             # Optional: Add a sleep to avoid overwhelming the server

if __name__ == "__main__":
    # Provide a list of CSV file paths
    csv_files = [
        'cid_1.csv',
        'cid_2.csv',
        'cid_3.csv',
        'cid_4.csv'
    ]
    
    process_catalogs_from_csv(csv_files)
