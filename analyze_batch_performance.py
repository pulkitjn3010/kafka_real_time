"""
Batch Performance Analyzer
Extracts metrics from Spark logs to analyze micro-batch behavior
"""

import re
import json
from datetime import datetime
from collections import defaultdict

def parse_spark_logs(log_file="spark_logs.txt"):
    """
    Parse Spark logs to extract batch metrics:
    - Batch processing time
    - Records per batch
    - Throughput
    - Skewness (variance in batch sizes)
    """
    
    batch_pattern = r"Processing Batch #(\d+)"
    count_pattern = r"Initial batch size: (\d+) rows"
    dedup_pattern = r"After deduplication: (\d+) rows"
    time_pattern = r"Batch #(\d+) Complete"
    
    batches = defaultdict(dict)
    current_batch = None
    
    print("Parsing Spark logs...")
    
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Start of batch
                match = re.search(batch_pattern, line)
                if match:
                    current_batch = int(match.group(1))
                    batches[current_batch]['start_time'] = datetime.now()
                
                # Initial count
                match = re.search(count_pattern, line)
                if match and current_batch is not None:
                    batches[current_batch]['initial_count'] = int(match.group(1))
                
                # After deduplication
                match = re.search(dedup_pattern, line)
                if match and current_batch is not None:
                    batches[current_batch]['deduped_count'] = int(match.group(1))
                
                # End of batch
                match = re.search(time_pattern, line)
                if match and current_batch is not None:
                    batches[current_batch]['end_time'] = datetime.now()
                    current_batch = None
    
    except FileNotFoundError:
        print(f"Log file not found: {log_file}")
        print("\nTo generate logs, run:")
        print("  docker-compose logs spark-job > spark_logs.txt")
        return None
    
    # Calculate statistics
    print(f"\n✓ Parsed {len(batches)} batches\n")
    
    if not batches:
        print("No batch data found in logs")
        return None
    
    # Analyze
    batch_sizes = []
    processing_times = []
    throughputs = []
    duplicate_rates = []
    
    print("Batch Analysis:")
    print("=" * 80)
    print(f"{'Batch':<8} {'Initial':<10} {'Deduped':<10} {'Duplicates':<12} {'Time (s)':<10} {'Throughput':<12}")
    print("=" * 80)
    
    for batch_id in sorted(batches.keys()):
        b = batches[batch_id]
        
        if 'initial_count' in b and 'deduped_count' in b:
            initial = b['initial_count']
            deduped = b['deduped_count']
            duplicates = initial - deduped
            dup_rate = (duplicates / initial * 100) if initial > 0 else 0
            
            batch_sizes.append(initial)
            duplicate_rates.append(dup_rate)
            
            if 'start_time' in b and 'end_time' in b:
                duration = (b['end_time'] - b['start_time']).total_seconds()
                throughput = deduped / duration if duration > 0 else 0
                processing_times.append(duration)
                throughputs.append(throughput)
                
                print(f"{batch_id:<8} {initial:<10} {deduped:<10} {duplicates:<12} {duration:<10.2f} {throughput:<12.1f}")
            else:
                print(f"{batch_id:<8} {initial:<10} {deduped:<10} {duplicates:<12} {'N/A':<10} {'N/A':<12}")
    
    print("=" * 80)
    
    # Summary statistics
    if batch_sizes:
        import statistics
        
        print("\nSummary Statistics:")
        print("-" * 40)
        print(f"Total Batches: {len(batch_sizes)}")
        print(f"\nBatch Sizes:")
        print(f"  Min: {min(batch_sizes)}")
        print(f"  Max: {max(batch_sizes)}")
        print(f"  Mean: {statistics.mean(batch_sizes):.1f}")
        print(f"  Median: {statistics.median(batch_sizes):.1f}")
        print(f"  Std Dev: {statistics.stdev(batch_sizes):.1f}" if len(batch_sizes) > 1 else "  Std Dev: N/A")
        
        # Calculate skewness
        if len(batch_sizes) > 1:
            mean_size = statistics.mean(batch_sizes)
            std_size = statistics.stdev(batch_sizes)
            cv = (std_size / mean_size) * 100  # Coefficient of variation
            print(f"  Skewness (CV): {cv:.1f}%")
            
            if cv < 20:
                print("  → Low skewness (consistent batch sizes) ✓")
            elif cv < 50:
                print("  → Moderate skewness")
            else:
                print("  → High skewness (uneven distribution) ⚠")
        
        if duplicate_rates:
            print(f"\nDuplicate Rate:")
            print(f"  Average: {statistics.mean(duplicate_rates):.1f}%")
            print(f"  Max: {max(duplicate_rates):.1f}%")
        
        if processing_times:
            print(f"\nProcessing Time:")
            print(f"  Min: {min(processing_times):.2f}s")
            print(f"  Max: {max(processing_times):.2f}s")
            print(f"  Mean: {statistics.mean(processing_times):.2f}s")
        
        if throughputs:
            print(f"\nThroughput:")
            print(f"  Min: {min(throughputs):.1f} records/sec")
            print(f"  Max: {max(throughputs):.1f} records/sec")
            print(f"  Mean: {statistics.mean(throughputs):.1f} records/sec")
    
    return batches

if __name__ == "__main__":
    import sys
    
    log_file = sys.argv[1] if len(sys.argv) > 1 else "spark_logs.txt"
    
    print("\n" + "=" * 80)
    print("Spark Batch Performance Analyzer")
    print("=" * 80 + "\n")
    
    parse_spark_logs(log_file)