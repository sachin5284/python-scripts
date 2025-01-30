import csv
import math
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Path to your CSV file
csv_file = 'deactivation.csv'

# API endpoint and headers
url = 'http://taxonomy-admin.prd.meesho.int/api/v1/internal/psm/valid/cron'
headers = {
    'Authorization': 'Token bYTFfK5Czo42zfhMmPQoUvXmWiSJ9fV8EbTKdQfDFL4A40tJ',
    'MEESHO-ISO-COUNTRY-CODE': 'IN',
    'Content-Type': 'application/json'
}

def update_PSM(psm_Request):
    print(psm_Request)
    req = psm_Request
    response = requests.post(url, headers=headers, data=json.dumps(req))
    if response.status_code == 200:
        return f"Successfully updated PSM valid status {psm_Request}"
    else:
        return f"Failed to update PSM valid status {psm_Request}. Status code: {response.status_code}, Response: {response.text}"

def process_batch(batch):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(update_PSM, psmRequest): psmRequest for psmRequest in batch}
        for future in as_completed(futures):
            # Optionally handle the result or errors here
            print(future.result())


batch_size = 10
psmUpdateRequests = []

# Read catalog IDs from CSV file
with open(csv_file, newline='') as file:
    reader = csv.DictReader(file)
    for row in reader:
        psmUpdateRequests.append(
             [
                 {
                    'supplier_id': int(row['supplier_id']),
                    'product_id': int(row['product_id']),
                    'valid': int(row['valid']),
                    'valid_change_type': 7,  
                    'reason': row['reason'],
                }
            ]
    )

# Split catalog IDs into batches
num_batches = math.ceil(len(psmUpdateRequests) / batch_size)
batches = [psmUpdateRequests[i*batch_size:(i+1)*batch_size] for i in range(num_batches)]

# Process each batch
for batch in batches:
    process_batch(batch)