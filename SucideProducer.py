import json
import csv
from kafka import KafkaProducer

def producer():
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    with open('who_suicide_statistics.csv') as file:
        reader = csv.DictReader(file, delimiter=",")
        for row in reader:
            producer.send(topic='test', value=row)
            producer.flush()

if __name__ == '__main__':
    producer()