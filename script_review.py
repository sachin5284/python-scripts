#!/usr/bin/python3

# pip install pymysql
import pymysql
import json
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Open database connection
connection = pymysql.connect(host=os.getenv('REVIEW_HOST'), 
    port=3306, 
    user=os.getenv('REVIEW_USER'), 
    password=os.getenv('REVIEW_PASSWORD'), 
    database='review')
#r = redis.Redis(host='localhost', port=6379, db=0)

class HelpfulData:
    def __init__(self, id, review_id, user_id, valid):
        self.id = id
        self.review_id = review_id
        self.user_id = user_id
        self.valid = valid

    def __str__(self):
        return str(self.__dict__)

    def dump(self):
        return  {
            'id': id,
            "review_id": review_id,
            "user_id": user_id,
            "valid": valid
        }
try:
    # prepare a cursor object using cursor() method
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    batchSize = 100
    offset = 0
    count = 500
    id=0
    while id<5570696:
        helpful_list = list()
        print(f'Count:{count}, ID: {id}, batchSize:{batchSize}')
        cursor.execute(f'SELECT * from helpful_reviews where id> {id} order by id limit {batchSize}')
        results = cursor.fetchall()
        if results is not None:
            count = len(results)
            for row in results:
                id = row['id']
                review_id = row['review_id']
                user_id = row['user_id']
                valid = row['valid']
                valid_str = 'true'
                if valid is not None:
                    valid_str = 'true' if valid == 1 else 'false'

                helpful = HelpfulData(id, review_id, user_id, valid)
                #print(helpful.__dict__)
                helpful_list.append(helpful)

        headers = {
            'Content-Type': "application/json",
            'client-id': os.getenv('REVIEW_CLIENT_ID'),
            'secret-key': os.getenv('REVIEW_CLIENT_SECRET'),
            'MEESHO-ISO-COUNTRY-CODE': "IN"
        }

        list_json = [o.__dict__ for o in helpful_list]
        data = {
            'data': list_json
        }

        #print(json.dumps(data))
        r = requests.post("http://localhost:8080/api/v1/reviews/helpful/refresh/cache", headers = headers, data=json.dumps(data))
        if r.status_code != 200 :
            print("failed for id {id}")
        offset = offset + batchSize
finally:
    print("Processing finised. Closing connection.")
    # disconnect from server
    connection.close()