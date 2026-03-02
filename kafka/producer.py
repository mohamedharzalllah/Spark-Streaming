# Write your kafka producer code here
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
)

topic = 'transactions_csv'

with open("C:\\Users\\lenovo\\OneDrive\\Documents\\m2 IASD\\flux de donnée\\tp\\Spark-Streaming\\data\\transactions.json", "r") as file:
    data = json.load(file)
    for line in data:
        message = json.dumps(line).encode("utf-8")
        # BLOCK until message is delivered
        future = producer.send(topic, message)
        # record_metadata = future.get(timeout=10)
        print("Sent:",message)
        # time.sleep(0.1)  # simulate delay between messages

producer.flush()
producer.close()

