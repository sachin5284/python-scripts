import csv
import json
from math import prod
import requests
import os
from dotenv import load_dotenv

load_dotenv()

file_path = "pidName.csv"  # Replace with the path to your JSON file

# Open the file for reading
with open(file_path, 'r') as file:
    # Iterate over each line in the file
    for line in file:
        try:
            # Parse the JSON object from the line
            request = {'product_id':int()}
            print(request)
            URL = os.getenv('TAXONOMY_ENDPOINT') + "/api/v1/product/edit"
            HEADERS = {"MEESHO-ISO-COUNTRY-CODE":"IN","Authorization":os.getenv('TAXONOMY_AUTH'),"Content-Type":"application/json"}
            r = requests.post(url = URL, data = json.dumps(request), headers = HEADERS)
            print(r.status_code)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {str(e)}")

        # extracting each data row one by one

        