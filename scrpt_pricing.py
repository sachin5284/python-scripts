import csv
import pymysql
import requests

# Define the API URL and headers
API_URL = 'http://taxonomy-admin.prd.meesho.int/api/v1/products/edit'
HEADERS = {
    'Authorization': 'Token bYTFfK5Czo42zfhMmPQoUvXmWiSJ9fV8EbTKdQfDFL4A40tJ',
    'MEESHO-ISO-COUNTRY-CODE': 'IN',
    'Content-Type': 'application/json'
}

def bulk_upsert_product(product_id):
    
    data = {
       "product_id": product_id,"update_product_images":False,
       "valid":1
    }

    response = requests.post(API_URL, headers=HEADERS, json=data)
    print(response.status_code)
    # Return response status and content
    return response.status_code, response.json()

def bulk_upsert_catalog(catalog_id):
    url = 'http://supplier-search.prd.meesho.int/api/v1/catalog-management/supplier/catalog/bulk-upsert'
    headers = {
        'Content-Type': 'application/json',
        'Authentication': 'kLSlxSTbsrrlK45GXvmGRGYK4QYiX6lpL6y9LQoSBkbH5qjC'
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


taxonomyConnection = pymysql.connect(host= 'msql-taxonomy-master.prd.meesho.int', port=3306, user='taxonomy', password='UQAufOdlT8Gf0r33OAmHg', database='taxonomy')
taxonomyCursor = taxonomyConnection.cursor()


file_path = 'pid_1.csv'
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file)
    
    # extracting field names through first row
    fields = next(csv_reader)
    for row in csv_reader:
        productId = int(row[0])
        print(productId)
        sql = f"SELECT valid,catalogId FROM products WHERE id = {productId}"
        taxonomyCursor.execute(sql)
        result = taxonomyCursor.fetchone()
        catalogId=result[1]
        if result[0]==0:
            bulk_upsert_product(productId)

        sql = f"SELECT valid FROM catalogs WHERE id = {catalogId}"
        taxonomyCursor.execute(sql)
        result = taxonomyCursor.fetchone()
        if result[0]==1:
            print("Catalog is active")
            print(catalogId)
            bulk_upsert_catalog(catalogId)
            send_deactivation_request(catalogId)