from kafka import KafkaConsumer
import json

TOPICS = [
    "debezium.public.customers",
    "debezium.public.products",
    "debezium.public.orders"
]

# Use the external Kafka address
BOOTSTRAP_SERVERS = ["localhost:29092"]

def process_debezium_message(msg):
    """Process Debezium message format and extract the actual data."""
    try:
        # The actual data is in the 'after' field of the payload
        if msg.get('payload', {}).get('after'):
            return msg['payload']['after']
        return msg
    except Exception as e:
        print(f"Error processing message: {e}")
        return msg

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
        processed_data = process_debezium_message(msg.value)
        print(json.dumps(processed_data, indent=2))

if __name__ == "__main__":
    main()
