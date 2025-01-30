import requests
from time import sleep

def bulk_upsert_catalog(catalog_ids):
    url = 'http://supplier-search.prd.meesho.int/api/v1/catalog-management/supplier/catalog/bulk-upsert'
    headers = {
        'Content-Type': 'application/json',
        'Authentication': 'kLSlxSTbsrrlK45GXvmGRGYK4QYiX6lpL6y9LQoSBkbH5qjC'
    }
    payload=[]
    for catalogId in catalog_ids:
        payload.append({"catalog_id": catalogId})
    data = {
        "payload": payload
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        print(f"Status Code {response.status_code}")
        return response.status_code, response.json()
    except requests.exceptions.RequestException as e:
        print(data)
        return None, None

def process_catalogs(start_id, end_id, batch_size=50):
    for i in range(start_id, end_id - 1, -batch_size):
        print(i)
        batch = range(i, max(i - batch_size, end_id - 1), -1)
        bulk_upsert_catalog(batch)
        sleep(1)  # Optional: Add a delay between batches to avoid overwhelming the server

if __name__ == "__main__":
    process_catalogs(132441135, 132441135)
