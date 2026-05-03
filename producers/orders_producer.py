from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send 100 messages then exit cleanly
for i in range(100):
    data = {
        "order_id": str(random.randint(1000, 9999)),
        "amount": random.randint(50, 500),
        "status": random.choice(["SUCCESS", "FAILED"]),
        "country": random.choice(["India", "USA"])
    }
    producer.send("orders", data)
    print("Sent:", data)
    time.sleep(2)

producer.flush()
producer.close()
print("Producer finished.")
