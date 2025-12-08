"""
Enhanced Spark Consumer with Deduplication Logic
Handles duplicate records in both Delta Lake and ClickHouse
"""

import sys
import time
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, date_format, md5, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable

print("=" * 60)
print("    Spark Multi-Sink Consumer with Deduplication")
print("=" * 60)

# --- Configuration ---
# Use container names for internal Docker networking
KAFKA_BROKER = "kafka-1:9093,kafka-2:9093"
INPUT_TOPIC = "raw_events"
OUTPUT_TOPIC = "processed_events"
DELTA_LAKE_PATH = "/delta-lake/events"
CHECKPOINT_PATH = "/tmp/multi_sink_checkpoint"

# Schema for incoming data
raw_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event", StringType(), True),
    StructField("timestamp", StringType(), True)
])

print(f"\nConfiguration:")
print(f"  Kafka Brokers: {KAFKA_BROKER}")
print(f"  Input Topic: {INPUT_TOPIC}")
print(f"  Output Topic: {OUTPUT_TOPIC}")
print(f"  Delta Lake Path: {DELTA_LAKE_PATH}")

# --- Create Spark Session ---
try:
    spark = (
        SparkSession.builder
        .appName("KafkaToMultiSinkWithDedup")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("\n✓ Spark Session created with Delta Lake support.")
except Exception as e:
    print(f"\n✗ Error creating Spark Session: {e}")
    sys.exit(1)

# --- Wait for Kafka Topics ---
def wait_for_kafka_topics(max_retries=30, retry_delay=5):
    print(f"\n⏳ Waiting for Kafka topics to be ready...")
    for attempt in range(1, max_retries + 1):
        try:
            print(f"  Attempt {attempt}/{max_retries}: Checking Kafka topics...")
            test_df = (
                spark.read
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER)
                .option("subscribe", INPUT_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
            )
            test_df.printSchema()
            print(f"✓ Kafka topic '{INPUT_TOPIC}' is ready!")
            return True
        except Exception as e:
            if attempt < max_retries:
                print(f"  ⚠ Not ready yet, waiting {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"\n✗ Failed after {max_retries} attempts")
                return False
    return False

if not wait_for_kafka_topics():
    sys.exit(1)

print("\n" + "=" * 60)
print("Kafka is ready! Starting streaming pipeline...")
print("=" * 60 + "\n")

# --- Batch Processing with Deduplication ---
def write_to_sinks_with_dedup(batch_df, batch_id):
    """
    Process micro-batch with deduplication:
    1. Remove duplicates WITHIN the batch
    2. For Delta Lake: Use MERGE to avoid duplicates
    3. For ClickHouse: Format timestamp correctly so Dedup works
    """
    print(f"\n{'=' * 60}")
    print(f"Processing Batch #{batch_id}")
    print(f"{'=' * 60}")
    
    if batch_df.isEmpty():
        print("⚠ Batch is empty, skipping.")
        return
    
    batch_df.cache()
    initial_count = batch_df.count()
    print(f"Initial batch size: {initial_count} rows")
    
    # --- STEP 1: Add composite key for deduplication ---
    # Composite key = user_id + event + timestamp
    batch_df_with_key = batch_df.withColumn(
        "dedup_key",
        md5(concat_ws("_", col("user_id"), col("event"), col("timestamp")))
    )
    
    # --- STEP 2: Remove duplicates WITHIN this batch ---
    batch_deduped = batch_df_with_key.dropDuplicates(["dedup_key"])
    deduped_count = batch_deduped.count()
    duplicates_in_batch = initial_count - deduped_count
    
    print(f"After deduplication: {deduped_count} rows")
    print(f"Duplicates removed: {duplicates_in_batch}")
    
    if batch_deduped.isEmpty():
        print("⚠ All records were duplicates, skipping writes.")
        batch_df.unpersist()
        return
    
    # Show sample
    print("\nSample data:")
    batch_deduped.select("user_id", "event", "timestamp").show(5, truncate=False)
    
    # --- SINK 1: Write to Kafka (for ClickHouse) ---
    try:
        print(f"\n[1/2] Writing to Kafka topic '{OUTPUT_TOPIC}'...")
        
        # Convert Timestamp to String format safely
        kafka_df = batch_deduped.withColumn(
            "timestamp_formatted", 
            date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        )
        
        # CRITICAL FIX: Rename 'timestamp_formatted' back to 'timestamp'
        # ClickHouse expects: {"user_id": 1, "event": "click", "timestamp": "..."}
        # If we send "timestamp_formatted", ClickHouse defaults timestamp to 1970-01-01
        kafka_output = kafka_df.select(
            to_json(struct(
                col("user_id"), 
                col("event"), 
                col("timestamp_formatted").alias("timestamp") 
            )).alias("value")
        )
        
        kafka_output.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", OUTPUT_TOPIC) \
            .save()
        
        print(f"✓ Wrote {deduped_count} rows to Kafka")
        
    except Exception as e:
        print(f"✗ Error writing to Kafka: {e}")
    
    # --- SINK 2: Write to Delta Lake with MERGE ---
    try:
        print(f"\n[2/2] Writing to Delta Lake at '{DELTA_LAKE_PATH}'...")
        
        # Prepare data (keep only necessary columns)
        delta_df = batch_deduped.select("user_id", "event", "timestamp", "dedup_key")
        
        # Check if Delta table exists
        from pyspark.sql.utils import AnalysisException
        
        try:
            # Table exists - use MERGE (upsert)
            delta_table = DeltaTable.forPath(spark, DELTA_LAKE_PATH)
            
            print("  Using MERGE to deduplicate against existing data...")
            
            # MERGE: Update if exists, Insert if new
            (delta_table.alias("target")
                .merge(
                    delta_df.alias("source"),
                    "target.dedup_key = source.dedup_key"  # Match on composite key
                )
                .whenMatchedUpdateAll()  # Update existing (idempotent)
                .whenNotMatchedInsertAll()  # Insert new
                .execute()
            )
            
            print(f"✓ MERGE complete (deduplicated against historical data)")
            
        except AnalysisException:
            # Table doesn't exist - first write
            print("  First write - creating Delta table...")
            delta_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(DELTA_LAKE_PATH)
            print(f"✓ Created Delta table with {deduped_count} rows")
        
        # Show Delta Lake statistics
        total_records = spark.read.format("delta").load(DELTA_LAKE_PATH).count()
        print(f"  Total records in Delta Lake: {total_records}")
        
    except Exception as e:
        print(f"✗ Error writing to Delta Lake: {e}")
        traceback.print_exc()
    
    batch_df.unpersist()
    print(f"\n{'=' * 60}")
    print(f"Batch #{batch_id} Complete")
    print(f"  Initial: {initial_count} | Deduped: {deduped_count} | Removed: {duplicates_in_batch}")
    print(f"{'=' * 60}\n")

# --- Read from Kafka ---
try:
    print("\nConnecting to Kafka stream...")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    print(f"✓ Connected to Kafka topic '{INPUT_TOPIC}'")
except Exception as e:
    print(f"✗ Error creating Kafka stream: {e}")
    sys.exit(1)

# --- Parse and Transform ---
print("\nParsing JSON data...")
parsed_df = (
    kafka_df
    .select(col("value").cast("string").alias("json_data"))
    .select(from_json(col("json_data"), raw_schema).alias("data"))
    .select(
        col("data.user_id").alias("user_id"),
        col("data.event").alias("event"),
        col("data.timestamp").cast(TimestampType()).alias("timestamp")
    )
)
print("✓ Data transformation configured.")

# --- Start Streaming Query ---
print("\nStarting multi-sink streaming query with deduplication...")
print(f"Trigger interval: 15 seconds")
print(f"Deduplication: Enabled (composite key: user_id + event + timestamp)")
print("\n" + "=" * 60)
print("STREAMING ACTIVE - Waiting for data...")
print("=" * 60 + "\n")

try:
    query = (
        parsed_df.writeStream
        .foreachBatch(write_to_sinks_with_dedup)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start()
    )
    
    query.awaitTermination()
    
except KeyboardInterrupt:
    print("\n\n✓ Streaming job stopped by user.")
except Exception as e:
    print(f"\n\n✗ Streaming job failed: {e}")
    sys.exit(1)