import csv
import pymysql
import requests
import concurrent.futures

# Define the API URL and headers
API_URL = 'http://taxonomy-admin.prd.meesho.int/api/v1/products/edit'
HEADERS = {
    'Authorization': 'Token bYTFfK5Czo42zfhMmPQoUvXmWiSJ9fV8EbTKdQfDFL4A40tJ',
    'MEESHO-ISO-COUNTRY-CODE': 'IN',
    'Content-Type': 'application/json'
}

def bulk_upsert_catalog(catalog_ids):
    url = 'http://supplier-search.prd.meesho.int/api/v1/catalog-management/supplier/catalog/bulk-upsert'
    headers = {
        'Content-Type': 'application/json',
        'Authentication': 'kLSlxSTbsrrlK45GXvmGRGYK4QYiX6lpL6y9LQoSBkbH5qjC'
    }
    payload = []
    for catalog_id in catalog_ids:
        payload.append({"catalog_id": catalog_id})
    data = {
        "payload": payload
    }

    response = requests.post(url, headers=headers, json=data)
    print(response.status_code)
    # Return response status and content
    return response.status_code, response.json()

def send_deactivation_request(entity_id):
    url = 'http://taxonomy-admin.prd.meesho.int/api/v1/deactivation/reason/trash'
    
    data = {
        'entity_type': 'CATALOG',
        'entity_id': entity_id
    }

    response = requests.post(url, headers=HEADERS, json=data)
    print(f"Entity ID: {entity_id}, Status Code: {response.status_code}")
    print(response.text)

def send_requests_in_parallel(entity_ids):
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(send_deactivation_request, entity_ids)
    # Return response status and content


taxonomyConnection = pymysql.connect(host= 'msql-taxonomy-data-slave.prd.meesho.int', port=3306, user='taxonomy', password='UQAufOdlT8Gf0r33OAmHg', database='taxonomy')
taxonomyCursor = taxonomyConnection.cursor()


def process_batch(batch):
    print(batch[0])
    catalogIds=[]
    catalogIdString= ','.join(map(str, batch))
    sql = f"SELECT id,valid from catalogs where id in (select catalogId FROM products WHERE id in ({catalogIdString}))"
    taxonomyCursor.execute(sql)
    result = taxonomyCursor.fetchall()
    for record in result:
        if record[1] == 1:
            catalogIds.append(record[0])
    if catalogIds:
        send_requests_in_parallel(catalogIds)
        bulk_upsert_catalog(catalogIds)

def read_and_process_in_batches(file_path, batch_size=100):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        fields = next(csv_reader)  # Read the header
        
        batch = []
        for row in csv_reader:
            batch.append(row[0])
            if len(batch) == batch_size:
                process_batch(batch)
                batch = []  # Reset batch after processing
        
        # Process any remaining rows
        if batch:
            try:
                process_batch(batch)
            except:
                process_batch(batch)
            
                

# Example usage:
file_path = 'pid_1.csv'
read_and_process_in_batches(file_path)