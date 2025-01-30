import json
import csv


rows=[]
with open("attribute_result.log", 'r') as file:
        # read all content of a file
        for l_no, line in enumerate(file):
            if line.find('product_id')!= -1:
                a = line[:line.find('{\'product_id')]
                b = line[line.find('product_id')-2:len(line)-1]
                try:
                    attributes = json.loads(b.replace("\'", "\""))
                except:
                    attributes = json.loads(str(b))
                rows.append({'product_id':attributes['product_id'],'request':str(b),'error':str(a)})

fields =['product_id','request','error']
with open("attribute_final.csv", 'w') as csvfile: 
    writer = csv.DictWriter(csvfile, fieldnames = fields) 
    writer.writeheader() 
    writer.writerows(rows) 