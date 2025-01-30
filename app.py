import csv
import pymysql
import requests

# Define the API URL and headers
API_URL = 'http://localhost/api/v1/products/edit'
HEADERS = {
    'Authorization': 'Token abc',
    'MEESHO-ISO-COUNTRY-CODE': 'IN',
    'Content-Type': 'application/json'
}

def bulk_upsert_catalog(catalog_id):
    url = 'http://gement/supplier/catalog/bulk-upsert'
    headers = {
        'Content-Type': 'application/json',
        'Authentication': ''
    }
    data = {
        "payload": [{"catalog_id": catalog_id}]
    }

    response = requests.post(url, headers=headers, json=data)
    print(response.status_code)
    # Return response status and content
    return response.status_code, response.json()

def send_deactivation_request( entity_id):
    url = 'http://taxonomy-admin.prd.meesho.int/api/v1/deactivation/reason/trash'
    
    data = {
        'entity_type': 'CATALOG',
        'entity_id': entity_id
    }

    response = requests.post(url, headers=HEADERS, json=data)
    print(response.status_code)
    print(response.text)
    # Return response status and content


taxonomyConnection = pymysql.connect(host= 'msql-', port=3306, user='taxonomy', password='UQAufOdlT8Gf0r33OAmHg', database='taxonomy')
taxonomyCursor = taxonomyConnection.cursor()

file_path = 'catalogIdsNew.csv'
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file)
    
    # extracting field names through first row
    fields = next(csv_reader)
    for row in csv_reader:
        catalogId = int(row[0])
        print(catalogId)
        sql = f"SELECT valid FROM catalogs WHERE id = {catalogId}"
        taxonomyCursor.execute(sql)
        result = taxonomyCursor.fetchone()
        if result[0]==1:
            send_deactivation_request(catalogId)
            bulk_upsert_catalog(catalogId)
            continue
