"""
Generate a static dataset of 10,000 user events for testing
This allows reproducible experiments and performance benchmarking
"""

import json
import random
from datetime import datetime, timedelta

def generate_static_dataset(num_records=10000, filename="test_data.json"):
    """
    Generate exactly 10,000 records with:
    - Controlled distribution of events
    - Intentional duplicates for deduplication testing
    - Realistic timestamps spanning a time window
    """
    
    print(f"Generating {num_records} records...")
    
    # Event distribution (realistic proportions)
    event_types = ['click', 'view', 'search', 'purchase']
    event_weights = [0.5, 0.3, 0.15, 0.05]  # 50% clicks, 30% views, 15% searches, 5% purchases
    
    # Start from a base time
    base_time = datetime(2025, 11, 21, 10, 0, 0)
    
    records = []
    
    for i in range(num_records):
        # Generate timestamp (spread events over 1 hour)
        seconds_offset = random.randint(0, 3600)
        event_time = base_time + timedelta(seconds=seconds_offset)
        
        # Select event type with weighted distribution
        event = random.choices(event_types, weights=event_weights)[0]
        
        # User ID distribution (100 unique users)
        user_id = random.randint(100, 199)
        
        # Create record
        record = {
            'user_id': user_id,
            'event': event,
            'timestamp': event_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        
        records.append(record)
    
    # INTENTIONALLY CREATE DUPLICATES for testing (10% duplicates)
    num_duplicates = int(num_records * 0.1)
    print(f"Adding {num_duplicates} duplicate records for deduplication testing...")
    
    for _ in range(num_duplicates):
        # Pick a random existing record and duplicate it
        duplicate = random.choice(records).copy()
        records.append(duplicate)
    
    # Shuffle to mix duplicates throughout the dataset
    random.shuffle(records)
    
    # Write to file
    with open(filename, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')  # JSONL format (one JSON per line)
    
    print(f"✓ Created {len(records)} records in '{filename}'")
    print(f"  - Unique records: ~{num_records}")
    print(f"  - Duplicates: ~{num_duplicates}")
    print(f"  - Total: {len(records)}")
    
    # Print statistics
    print("\nEvent Distribution:")
    from collections import Counter
    event_counts = Counter(r['event'] for r in records)
    for event, count in event_counts.most_common():
        print(f"  {event}: {count} ({count/len(records)*100:.1f}%)")
    
    user_counts = Counter(r['user_id'] for r in records)
    print(f"\nUnique Users: {len(user_counts)}")
    print(f"Most Active User: {user_counts.most_common(1)[0][0]} ({user_counts.most_common(1)[0][1]} events)")

if __name__ == "__main__":
    generate_static_dataset(num_records=10000, filename="test_data_10k.json")