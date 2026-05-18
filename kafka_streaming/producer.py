import os
import sys
import json
import time
import logging
import argparse
from datetime import date
import mysql.connector
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("kafka.producer")

#kafka and MySQL connection settings from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "armed_conflict_metrics")
MYSQL_HOST              = os.getenv("MYSQL_DW_HOST", "mysql_dw")
MYSQL_PORT              = int(os.getenv("MYSQL_DW_PORT", "3306"))
MYSQL_USER              = os.getenv("MYSQL_DW_USER", "etl_user")
MYSQL_PASSWORD          = os.getenv("MYSQL_DW_PASSWORD", "etl_password")
MYSQL_DB                = os.getenv("MYSQL_DW_DB", "dw_armed_conflict")

#pulls the full victim records joined across all dimension tables
QUERY = """
SELECT
    v.total_victim,
    v.source,
    rd.date_processing,
    rd.year,
    rd.month,
    l.state_dept,
    va.victimization_fact,
    p.sex,
    p.ethnic_group,
    p.age_range
FROM victims v
JOIN person p  ON v.id_person = p.id_person
JOIN victimizing_act va ON v.id_act = va.id_act
JOIN location l  ON v.id_location = l.id_location
JOIN registration_date rd ON v.date_processing = rd.date_processing
ORDER BY rd.date_processing ASC, v.source ASC
"""


def json_serializer(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Type not serializable: {type(obj)}")
#connects to MySQL with retries in case the service isn't ready yet
def get_mysql_connection(retries=5, wait=5):
    for attempt in range(1, retries + 1):
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB,
                connection_timeout=10,
            )
            log.info("MySQL connection successful (attempt %d/%d).", attempt, retries)
            return conn
        except mysql.connector.Error as exc:
            log.warning("MySQL connection failed (attempt %d/%d): %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(wait)
    log.error("Could not connect to MySQL after %d attempts.", retries)
    sys.exit(1)


#connects to Kafka with retries in case the broker isn't ready yet
def get_kafka_producer(retries=5, wait=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=json_serializer).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
                max_block_ms=10_000,
            )
            log.info("KafkaProducer connected to %s (attempt %d/%d).", KAFKA_BOOTSTRAP_SERVERS, attempt, retries)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka broker not available (attempt %d/%d). Waiting %ds...", attempt, retries, wait)
            if attempt < retries:
                time.sleep(wait)
    log.error("Could not connect to Kafka after %d attempts.", retries)
    sys.exit(1)


#Log delivery success every 500 messages and report errors immediately
def on_send_success(record_metadata, idx):
    if idx % 500 == 0:
        log.info(
            "Message #%d -> topic=%s partition=%d offset=%d",
            idx, record_metadata.topic, record_metadata.partition, record_metadata.offset,
        )


def on_send_error(exc, idx):
    log.error("Error sending message #%d: %s", idx, exc)

#Reads from MySQL in batches and publishes each row to the Kafka topic
def run(delay=0.05, batch_size=50):
    log.info("Kafka Producer started")
    log.info("Topic: %s | Delay: %ss | Batch size: %d", KAFKA_TOPIC, delay, batch_size)

    conn     = get_mysql_connection()
    producer = get_kafka_producer()
    cursor   = conn.cursor(dictionary=True)

    log.info("Running query on fact table...")
    cursor.execute(QUERY)

    sent     = 0
    errors   = 0
    start_ts = time.time()

    try:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                row["produced_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                future = producer.send(KAFKA_TOPIC, value=row)
                future.add_callback(on_send_success, sent)
                future.add_errback(on_send_error, sent)
                sent += 1
                time.sleep(delay)

            producer.flush()

            elapsed = time.time() - start_ts
            log.info("Progress: %d messages sent | %.1f msg/s", sent, sent / elapsed if elapsed > 0 else 0)

    except KeyboardInterrupt:
        log.warning("Production interrupted by user.")
    except KafkaError as exc:
        log.error("Kafka error: %s", exc)
        errors += 1
    finally:
           #close all connections and log final stats
        producer.flush()
        producer.close()
        cursor.close()
        conn.close()

        elapsed = time.time() - start_ts
        log.info(
            "Production finished\n"
            "  Messages sent : %d\n"
            "  Errors        : %d\n"
            "  Total time    : %.2f s\n"
            "  Throughput    : %.1f msg/s",
            sent, errors, elapsed, sent / elapsed if elapsed > 0 else 0,
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka producer - armed conflict metrics")
    parser.add_argument("delay",      type=float, default=0.05, help="Delay in seconds between messages (default: 0.05)")
    parser.add_argument("batch-size", type=int,   default=50,   help="Rows per fetchmany/flush cycle (default: 50)")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run(delay=args.delay, batch_size=args.batch_size)