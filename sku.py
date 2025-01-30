import csv
import json
import requests

# Path to your CSV file
csv_file_path = 'psmAttributes.csv'

# API URL
url = 'http://taxonomy-admin.prd.meesho.int/api/v1/psm/attributes/upsert/bulk'

# Headers for the API request
headers = {
    'Authorization': 'Token bYTFfK5Czo42zfhMmPQoUvXmWiSJ9fV8EbTKdQfDFL4A40tJ',
    'MEESHO-ISO-COUNTRY-CODE': 'IN',
    'Content-Type': 'application/json'
}

# Read the CSV file and prepare the payload in batches
batch_size = 10
payload = []

def send_batch(payload_batch):
    """Function to send a batch of data to the API."""
    data = {"payload": payload_batch}
    json_data = json.dumps(data)
    response = requests.post(url, headers=headers, data=json_data)
    
    # Print the response from the server
    print(f"Batch sent. Status code: {response.status_code}")
    try:
        print(response.json())
    except ValueError:
        print("Response not in JSON format")

with open(csv_file_path, mode='r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        data = {
            "product_id": row['product_id'],
            "supplier_id": row['supplier_id'],
            "attribute_name": row['attribute_name'],
            "attribute_value": ''
        }
        payload.append(data)
        
        # Send the batch when the batch size is reached
        if len(payload) == batch_size:
            send_batch(payload)
            payload = []  # Clear the payload for the next batch

    # Send any remaining data that didn't make up a full batch
    if payload:
        send_batch(payload)
