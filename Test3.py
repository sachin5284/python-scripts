
import pymysql
import csv
import json

def partition(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

data =[]
map = {}
isComma = 0
with open('Schema Additions 19-11 - Schema Additions  - Schema Additions 19-11 - Schema Additions .csv') as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        key = rows['L1']+rows['L2']+rows['L3']+rows['L4']+rows['Attribute_Field_Name'];
        if map.__contains__(key):
            map.get(key)['Value List'] = map.get(key)['Value List'] + ";" + rows['Value List']
        
        else:
            map[key] = rows

for key in map:
    data.append(map[key])

chunks = list(partition(data, 50))
i = 0
for record in chunks:
    i = i+1
    fileName = 'Schema Additions 19-11 - Schema Additions  - Schema Additions 19-11 - Schema Additions' + str(i) +'.csv'
    with open(fileName,'w') as out:
        csv_columns =['L1','L2','L3','L4','Attribute_Field_Name','Attribute Display Name','Required (TRUE/FLASE)','Value List']
        writer = csv.DictWriter(out, fieldnames=csv_columns)
        writer.writeheader()
        for val in record:
            writer.writerow(val)



