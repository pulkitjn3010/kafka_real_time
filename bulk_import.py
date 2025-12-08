import json
import time
from kafka import KafkaProducer

# Connect to the External Listeners (localhost)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=16384,
    linger_ms=10
)

topic = 'raw_events'
filename = 'test_data_10k.json'

print(f"Reading {filename} and sending to Kafka...")

count = 0
start_time = time.time()

try:
    with open(filename, 'r') as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                # Send without waiting (async) for speed
                producer.send(topic, value=record)
                count += 1
                
                if count % 1000 == 0:
                    print(f"Sent {count} records...")

    print("Flushing remaining messages...")
    producer.flush()
    
    duration = time.time() - start_time
    print(f"✓ Successfully sent {count} records in {duration:.2f} seconds.")

except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()