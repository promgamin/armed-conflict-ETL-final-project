import os
import sys
import json
import time
import logging
import argparse

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("kafka.consumer")

# env variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "armed_conflict_metrics")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "armed_conflict_group")


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


# kafka consumer 
def get_kafka_consumer(retries=5, wait=5):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=json_deserializer,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
            )
            log.info("KafkaConsumer connected to %s (attempt %d/%d).", KAFKA_BOOTSTRAP_SERVERS, attempt, retries)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka broker not available (attempt %d/%d). Waiting %ds...", attempt, retries, wait)
            if attempt < retries:
                time.sleep(wait)
    log.error("Could not connect to Kafka after %d attempts.", retries)
    sys.exit(1)


def format_record(record: dict) -> str:
    return (
        f"[{record.get('date_processing', 'N/A')}] "
        f"{record.get('state_dept', 'N/A')} | "
        f"{record.get('victimization_fact', 'N/A')} | "
        f"victims={record.get('total_victim', 0)} | "
        f"source={record.get('source', 'N/A')}"
    )


def run(output_path=None):
    log.info("Kafka Consumer started")
    log.info("Topic: %s | Group: %s | Offset: latest", KAFKA_TOPIC, KAFKA_GROUP_ID)

    consumer = get_kafka_consumer()

    received = 0
    errors   = 0
    start_ts = time.time()

    out_file = open(output_path, "w", encoding="utf-8") if output_path else None
    if out_file:
        log.info("Writing consumed messages to: %s", output_path)

    try:
        # poll() keeps the consumer alive 
        while True:
            message_batch = consumer.poll(timeout_ms=1000) # timeout_ms=1000 wait up 1 second per poll cycle before returning empty

            if not message_batch:
                log.debug("No messages in this poll cycle, still listening...")
                continue

            for _, messages in message_batch.items():
                for message in messages:
                    try:
                        record = message.value
                        received += 1

                        if received % 500 == 0:
                            elapsed = time.time() - start_ts
                            log.info(
                                "Received #%d | partition=%d offset=%d | %.1f msg/s",
                                received, message.partition, message.offset,
                                received / elapsed if elapsed > 0 else 0,
                            )

                        log.debug("Record: %s", format_record(record))

                        if out_file:
                            out_file.write(json.dumps(record, default=str) + "\n")

                    except Exception as exc:
                        log.error("Error processing message: %s", exc)
                        errors += 1

    except KeyboardInterrupt:
        log.warning("Consumer interrupted by user.")
    except KafkaError as exc:
        log.error("Kafka error: %s", exc)
        errors += 1
    finally:
        consumer.close()
        if out_file:
            out_file.close()

        elapsed = time.time() - start_ts
        log.info(
            "Consumer finished\n"
            "Messages received : %d\n"
            "Errors: %d\n"
            "Total time: %.2f s\n"
            "Throughput: %.1f msg/s",
            received, errors, elapsed, received / elapsed if elapsed > 0 else 0,
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka consumer - armed conflict metrics")
    parser.add_argument("--output", type=str, default=None, help="Optional path to write consumed messages as JSONL")
    return parser.parse_args()