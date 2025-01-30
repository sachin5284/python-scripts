import csv
import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

headers = {
    'Authorization': os.getenv('TAXONOMY_AUTH'),
    'MEESHO-ISO-COUNTRY-CODE': 'IN',
    'MEESHO-ISO-LANGUAGE-CODE': 'EN',
    'Content-Type': 'application/json'
}

def get_parent_supplier_id(product_id):
    try:
        url = os.getenv('TAXONOMY_ENDPOINT') + '/api/v1/psm/fetch/bulk'
        payload = {
            "valid_only": False,
            "product_ids": [product_id]
        }
        data = json.dumps(payload)

        response = requests.post(url, headers=headers, data=data, timeout=4)
        if response.status_code == 200:
            content = json.loads(response.content)
            min_psm_id = 3000000000
            min_sup_id = 0
            for item in content['items']:
                if min_psm_id > item['id']:
                    min_psm_id = item['id']
                    min_sup_id = item['supplier_id']
            return min_sup_id
        else:
            return 0
    except Exception as e:
        print("Exception occurred: %s" % (str(e)))
        return 0

def update_vsku_data(product_id, supplier_id, vsku_data):
    if not vsku_data:
        print("No vsku data for product %s" % str(product_id))
        return
    try:
        url = os.getenv('TAXONOMY_ENDPOINT') + '/api/v1/vskus/admin/create'
        payload = {
            "data": []
        }

        for key, value in vsku_data.items():
            payload["data"].append({
                "product_id": product_id,
                "supplier_id": supplier_id,
                "variation_id": key,
                "vsku": value
            })
        data = json.dumps(payload)

        response = requests.post(url, headers=headers, data=data, timeout=4)
        if response.status_code == 200:
            pass
        else:
            print(" service failure response: %s", str(response.content))
    except Exception as e:
        print("Exception occurred: %s" % (str(e)))
    finally:
        time.sleep(0.05)

def get_skus(psid, product_id):
    try:
        url = os.getenv('TAXONOMY_ENDPOINT') + '/api/v1/vskus/fetch'
        payload = {
            "product_supplier_filters": [
                {
                    "product_id": product_id,
                    "supplier_id": psid
                }
            ]
        }
        data = json.dumps(payload)

        response = requests.post(url, headers=headers, data=data, timeout=4)
        if response.status_code == 200:
            content = json.loads(response.content)
            vsku_data = {}
            for item in content['vskus']:
                vsku_data[item["variation"]["id"]] = item["vsku"]
            return vsku_data
        else:
            return {}
    except Exception as e:
        print("Exception occurred: %s" % (str(e)))
    return {}

def main():
    file_path = 'sample-file/sp.csv'
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line = 0
        for row in csv_reader:
            supplier_id, product_id = row
            psid = get_parent_supplier_id(product_id)
            if psid == 0:
                continue

            vsku_data = get_skus(psid, product_id)
            update_vsku_data(product_id, supplier_id, vsku_data)
            line += 1
        print("Done for %s" % str(line))

main()