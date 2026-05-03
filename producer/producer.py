import time
import json
import random
from confluent_kafka import Producer

event_types = ["product_view", "add_to_cart", "purchase"]

conf = {
    "bootstrap.servers": "kafka:9092",
    "client.id": "customer-event-producer"
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()}")

print("Waiting for Kafka to be ready...")

# Retry loop instead of fixed sleep
while True:
    try:
        producer.list_topics(timeout=5)
        print("Kafka is ready!")
        break
    except Exception as e:
        print("Kafka not ready yet, retrying...")
        time.sleep(5)

# Start producing
while True:
    event = {
    "user_id": f"user_{random.randint(1, 10)}",
    "event_type": random.choice(event_types),
    "product_id": f"product_{random.randint(1, 50)}",
    "timestamp": time.time()
}

    producer.produce(
        topic="customer_events",
        value=json.dumps(event),
        callback=delivery_report
    )

    producer.poll(0)  # handles delivery callbacks

    print("Produced:", event)

    time.sleep(2)