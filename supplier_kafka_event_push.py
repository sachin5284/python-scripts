import json
import csv

from kafka import KafkaProducer
from kafka.errors import KafkaError

bootstrap_servers = "bac-p-shared-kafka-kb1.meeshoint.in:9092,bac-p-shared-kafka-kb2.meeshoint.in:9092," \
                    "bac-p-shared-kafka-kb3.meeshoint.in:9092,bac-p-shared-kafka-kb4.meeshoint.in:9092," \
                    "bac-p-shared-kafka-kb5.meeshoint.in:9092,bac-p-shared-kafka-kb6.meeshoint.in:9092"

topic = "supplierstore.supplier.upsert"

producer = KafkaProducer(bootstrap_servers=['bac-p-shared-kafka-kb1.meeshoint.in:9092','bac-p-shared-kafka-kb2.meeshoint.in:9092','bac-p-shared-kafka-kb3.meeshoint.in:9092','bac-p-shared-kafka-kb4.meeshoint.in:9092','bac-p-shared-kafka-kb5.meeshoint.in:9092','bac-p-shared-kafka-kb6.meeshoint.in:9092'],value_serializer=lambda x: json.dumps(x).encode('utf-8'),key_serializer=lambda k: k.encode('utf-8'))


def default_headers():
    return [("MEESHO-ISO-COUNTRY-CODE", bytes("IN", 'utf-8'))]

def push_to_kafka(supplier_id):
    data = {
    "meta": {
      "key": None,
      "topic": None,
      "service": "supplier_store",
      "async": None,
      "requestId": "b3554ccf-0bbe-4f2e-9fec-4a201b0c4f5e",
      "requestTimestamp": None
    },
    "data": json.dumps({
      "supplier_id": supplier_id,
      "op_type": "DELETE",
      "old_data": {
        "valid": True,
        "name": "UYTUYTYUUIYII",
        "valid_change_type": 1,
        "returns": 1,
        "qc_status_id": 1
      },
      "new_data": {
        "valid": False,
        "name": "UYTUYTYUUIYII",
        "valid_change_type": 1,
        "returns": 1,
        "qc_status_id": 1,
        "deactivation_reason":
      },
      "moderator": "script@meesho.com"
    })
  }

    future = producer.send(topic, value=data, key=str(supplier_id), headers=default_headers())
    try:
        future.get(timeout=10)
    except KafkaError:
        print(KafkaError)
        pass

def main():
    file_path = 'result.csv'
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file)
        
        # extracting field names through first row
        fields = next(csv_reader)
        for line in csv_reader:
            supplier_id = int(line[0])
            print("Populating for ", supplier_id)
            push_to_kafka(supplier_id)
            break

main()