#!/bin/bash

set -e

NUM_SENDERS=1
NUM_RECEIVERS_PER_SENDER=1
NUM_MESSAGES=100000
HWM=10000
BATCH_SLEEP_MS=100

NUM_DEALER_PAIRS=1
DEALER_NUM_MESSAGES=100000

PAYLOAD_SIZES=(8 64 256 1024 4096 16384 65536 262144 1048576 2097152)

TARGET_DIR="./target/release"

rm -f results.csv *.hgrm

echo "Building project..."
cargo build --release

echo "Starting benchmarks..."
echo "PUB/SUB Configuration: $NUM_SENDERS senders, $NUM_RECEIVERS_PER_SENDER receivers per sender, $NUM_MESSAGES messages"
echo "DEALER Configuration: $NUM_DEALER_PAIRS pairs, $DEALER_NUM_MESSAGES messages"
echo "High Water Mark: $HWM, Batch sleep: ${BATCH_SLEEP_MS}ms (PUB/SUB only)"
echo "Using TSC (Time Stamp Counter) for zero-syscall timestamping"
echo ""

run_pubsub_benchmark() {
    local transport=$1
    local size=$2

    echo "Running PUB/SUB ${transport^^} - Payload: $size bytes"

    $TARGET_DIR/zmq-bench pubsub-benchmark \
        --num-senders $NUM_SENDERS \
        --num-receivers-per-sender $NUM_RECEIVERS_PER_SENDER \
        --num-messages $NUM_MESSAGES \
        --payload-size $size \
        --transport $transport \
        --hwm $HWM \
        --batch-sleep-ms $BATCH_SLEEP_MS

    $TARGET_DIR/zmq-bench aggregator \
        --pattern "PubSub" \
        --transport "${transport^^}" \
        --payload-size $size

    echo "  Completed PUB/SUB ${transport^^} - Payload: $size bytes"
    echo ""
}

run_dealer_benchmark() {
    local transport=$1
    local size=$2

    echo "Running DEALER ${transport^^} - Payload: $size bytes"

    $TARGET_DIR/zmq-bench dealer-benchmark \
        --num-pairs $NUM_DEALER_PAIRS \
        --num-messages $DEALER_NUM_MESSAGES \
        --payload-size $size \
        --transport $transport

    $TARGET_DIR/zmq-bench aggregator \
        --pattern "Dealer" \
        --transport "${transport^^}" \
        --payload-size $size

    echo "  Completed DEALER ${transport^^} - Payload: $size bytes"
    echo ""
}

echo "=== PUB/SUB Benchmarks ==="
for size in "${PAYLOAD_SIZES[@]}"; do
    run_pubsub_benchmark "ipc" $size
    rm -f /tmp/pub_*.ipc
done

for size in "${PAYLOAD_SIZES[@]}"; do
    run_pubsub_benchmark "tcp" $size
done

echo "=== DEALER Benchmarks ==="
for size in "${PAYLOAD_SIZES[@]}"; do
    run_dealer_benchmark "ipc" $size
    rm -f /tmp/dealer_*.ipc
done

for size in "${PAYLOAD_SIZES[@]}"; do
    run_dealer_benchmark "tcp" $size
done

echo "All benchmarks complete! Results in results.csv"
