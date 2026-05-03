from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

countries = ["India", "USA", "UK", "Germany"]
status_list = ["SUCCESS", "FAILED"]

while True:
    data = {
        "order_id": str(random.randint(1000, 9999)),
        "amount": random.randint(50, 500),
        "status": random.choice(status_list),
        "country": random.choice(countries)
    }

    producer.send("orders", data)
    print("Sent:", data)

    time.sleep(2)
