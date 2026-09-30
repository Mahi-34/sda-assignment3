from kafka import KafkaConsumer
import mysql.connector
import json
from datetime import datetime

# -----------------------------
# Kafka settings
# -----------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC = "food-delivery-orders"
OUTPUT_FILE = "stream_data.jsonl"

# -----------------------------
# MySQL settings
# -----------------------------
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "Fore#1234"
MYSQL_DATABASE = "food_delivery"

# -----------------------------
# Connect to MySQL
# -----------------------------
db = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)

cursor = db.cursor()

print("Connected to MySQL successfully.")

# -----------------------------
# Connect to Kafka
# -----------------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="assignment3-consumer-v4",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("=" * 60)
print("ASSIGNMENT 3 - KAFKA CONSUMER")
print("=" * 60)
print(f"Listening to topic: {TOPIC}")
print("Saving events to JSONL and MySQL...")
print("Waiting for streaming events...")
print("Press Ctrl+C to stop.")
print("=" * 60)

# -----------------------------
# Receive Kafka events
# -----------------------------
with open(OUTPUT_FILE, "a", encoding="utf-8") as file:

    try:
        for message in consumer:

            event = message.value

            # Add consumer timestamp
            event["consumer_timestamp"] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            # -----------------------------
            # Save to JSONL
            # -----------------------------
            file.write(json.dumps(event) + "\n")
            file.flush()

            # -----------------------------
            # Save to MySQL
            # -----------------------------
            sql = """
                INSERT INTO delivery_events (
                    order_id,
                    delivery_person_id,
                    delivery_person_age,
                    delivery_person_rating,
                    city,
                    weather_conditions,
                    road_traffic_density,
                    vehicle_condition,
                    order_type,
                    vehicle_type,
                    multiple_deliveries,
                    festival,
                    time_taken_min,
                    event_type,
                    event_timestamp,
                    consumer_timestamp
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            values = (
                event.get("order_id"),
                event.get("delivery_person_id"),
                event.get("delivery_person_age"),
                event.get("delivery_person_rating"),
                event.get("city"),
                event.get("weather_conditions"),
                event.get("road_traffic_density"),
                event.get("vehicle_condition"),
                event.get("order_type"),
                event.get("vehicle_type"),
                event.get("multiple_deliveries"),
                event.get("festival"),
                event.get("time_taken_min"),
                event.get("event_type"),
                event.get("event_timestamp"),
                event.get("consumer_timestamp")
            )

            cursor.execute(sql, values)
            db.commit()

            # -----------------------------
            # Console output
            # -----------------------------
            print(
                f"Received: {event.get('event_type', 'UNKNOWN'):18} | "
                f"Order: {event.get('order_id', 'UNKNOWN')} | "
                f"MySQL: saved"
            )

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    finally:
        consumer.close()
        cursor.close()
        db.close()

        print("Kafka consumer closed.")
        print("MySQL connection closed.")