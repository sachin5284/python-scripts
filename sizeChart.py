import csv
import requests
import json

# API URL
api_url = 'http://supplier-store-admin.prd.meesho.int/api/v3/supplier/update'

# Headers for the request
headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': '1234',  # Replace with actual authorization token
    'moderator': 'nikhil.patwari@meesho.com'
}

# Path to the CSV file
csv_file_path = '1.csv'  # Replace with your CSV file path

# Reading supplier IDs from the CSV file
try:
    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        
        # Check if the 'supplier_id' column exists
        if 'supplier_id' not in csv_reader.fieldnames:
            raise ValueError("CSV file must contain a 'supplier_id' column.")
        
        # Iterate over each row and send the update request
        for row in csv_reader:
            supplier_id = row['supplier_id']

            # Data to be sent in the API request
            data = {
                "create_supplier_request": {
                    "supplier_id": int(supplier_id),
                    "valid": False
                },
                "supply_db_supplier_upsert_request": {
                    "supplier_id": int(supplier_id),
                    "valid": True
                }
            }

            try:
                response = requests.post(api_url, headers=headers, data=json.dumps(data))
                if response.status_code == 200:
                    print(f"Successfully updated supplier_id {supplier_id}.")
                    print("Response:", response.json())
                else:
                    print(f"Failed to update supplier_id {supplier_id}. Status code:", response.status_code)
                    print("Response:", response.text)
            except requests.exceptions.RequestException as e:
                print(f"An error occurred while updating supplier_id {supplier_id}: ", e)
            
            data = {
                "create_supplier_request": {
                    "supplier_id": int(supplier_id),
                    "valid": False
                },
                "supply_db_supplier_upsert_request": {
                    "supplier_id": int(supplier_id),
                    "valid": False
                }
            }

            try:
                response = requests.post(api_url, headers=headers, data=json.dumps(data))
                if response.status_code == 200:
                    print(f"Successfully updated supplier_id {supplier_id}.")
                    print("Response:", response.json())
                else:
                    print(f"Failed to update supplier_id {supplier_id}. Status code:", response.status_code)
                    print("Response:", response.text)
            except requests.exceptions.RequestException as e:
                print(f"An error occurred while updating supplier_id {supplier_id}: ", e)

except Exception as e:
    print("An error occurred: ", e)
