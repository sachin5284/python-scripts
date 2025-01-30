import pymysql
import csv
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import random
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from datetime import datetime


taxonomyConnection = pymysql.connect(host= os.getenv('TAXONOMY_RDS'), port=3306, user=os.getenv('TAXONOMY_USER'), password=os.getenv('TAXONOMY_PASSWORD'), database='taxonomy')
taxonomyCursor = taxonomyConnection.cursor()

# Connect to the Scylla cluster
cluster = Cluster(['172.28.112.165'])

# Create a session to interact with the database
session = cluster.connect('taxonomy')

def partition_list(input_list, chunk_size):
    for i in range(0, len(input_list), chunk_size):
        yield input_list[i:i + chunk_size]

id=8809022
hasnxt = True
products_to_insert=[]
while id<10000000:
    sql = """SELECT id, 
        valid, 
        name, 
        description, 
        catalogId, 
        fullCatalog,
        weight, 
        sku, 
        images,
        mrpPrice, 
        carrierWeight,
        created, rowLastUpdated FROM products WHERE id>%s limit 10000"""
    taxonomyCursor.execute(sql, (id))
    result = taxonomyCursor.fetchall()
    for record in result: 
        id=int(record[0])
        print(id)
        product={
            "productId": int(record[0]),
            "valid": int(record[1]) == 1,
            "name": record[2],
            "description": record[3],
            "catalogId": record[4],
            "fullCatalog": record[5],
            "weight": record[6],
            "subSubCategoryId": random.randrange(10000, 20000, 2),
            "sku": record[7],
            "images": record[8].split(','),
            "attributes": {"bottom_color": "Blue", "fabric": "Cotton","fabric": "Cotton","fit_shape": "Not Available","hemline": "Straight","length": "Regular"},
            "mrpPrice": record[9],
            "carrierWeight": record[10],
            "reverseCarrierWeight": record[10],
            "sortRank": random.randrange(1, 10, 1),
            "created": str(record[11]),
            "updated": str(record[12])
        }
        products_to_insert.append(product)
    insert_query = """
        INSERT INTO taxonomy.products (
            productId, valid, name, description, catalogId, fullCatalog,
            weight, subSubCategoryId, sku, images, attributes,
            mrpPrice, carrierWeight, reverseCarrierWeight, sortRank,
            created, updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    productBatches = list(partition_list(products_to_insert, 500))
    for products in productBatches:
        batch = BatchStatement()
        for product_data in products:
            batch.add(SimpleStatement(insert_query), (
                product_data["productId"],
                product_data["valid"],
                product_data["name"],
                product_data["description"],
                product_data["catalogId"],
                product_data["fullCatalog"],
                product_data["weight"],
                product_data["subSubCategoryId"],
                product_data["sku"],
                product_data["images"],
                product_data["attributes"],
                product_data["mrpPrice"],
                product_data["carrierWeight"],
                product_data["reverseCarrierWeight"],
                product_data["sortRank"],
                product_data["created"],
                product_data["updated"]
            ))
        session.execute(batch)
    products_to_insert=[]
# Close the session and cluster when done
session.shutdown()
cluster.shutdown()
