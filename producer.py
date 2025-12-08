import json
import time
import random
from kafka import KafkaProducer

print("Attempting to connect to Kafka brokers...")

try:
    # FIXED: List of separate strings, no space after comma
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092', 'localhost:9094'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Wait for all replicas to acknowledge
        retries=3     # Retry on failure
    )
    print("✓ Kafka Producer connected successfully to both brokers.")
except Exception as e:
    print(f"✗ Error connecting to Kafka Producer: {e}")
    print("Please make sure docker-compose services are running:")
    print("  docker-compose up -d")
    exit(1)

TOPIC_NAME = 'raw_events'
print(f"\nProducer starting. Sending messages to '{TOPIC_NAME}'...")
print("(Press Ctrl+C to stop)\n")

event_types = ['click', 'view', 'search', 'purchase']
message_count = 0

try:
    while True:
        message = {
            'user_id': random.randint(100, 1000),
            'event': random.choice(event_types),
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        }
        
        # Send with callback to verify delivery
        future = producer.send(TOPIC_NAME, value=message)
        
        # Wait for send to complete (optional, for reliability)
        try:
            record_metadata = future.get(timeout=10)
            message_count += 1
            print(f"[{message_count}] Sent to partition {record_metadata.partition}: {message}")
        except Exception as e:
            print(f"✗ Failed to send message: {e}")
        
        time.sleep(1)

except KeyboardInterrupt:
    print(f"\n\nProducer stopped by user. Sent {message_count} messages total.")
except Exception as e:
    print(f"\n✗ An error occurred: {e}")
finally:
    print("Flushing and closing producer...")
    producer.flush()
    producer.close()
    print("✓ Producer closed gracefully.")