from kafka import KafkaConsumer
import json

TOPICS = [
    "debezium.public.customers",
    "debezium.public.products",
    "debezium.public.orders"
]

BOOTSTRAP_SERVERS = ["localhost:9092"]

def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',  # or 'latest'
        enable_auto_commit=True,
        group_id='json-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    print(f"✅ Listening to topics: {', '.join(TOPICS)}\n")
    for msg in consumer:
        print(f"\n📦 Topic: {msg.topic}")
        print(json.dumps(msg.value, indent=2))

if __name__ == "__main__":
    main()
