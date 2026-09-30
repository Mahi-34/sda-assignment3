import json
import time
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError


# =========================================================
# CONFIGURATION
# =========================================================

KAFKA_BROKER = "localhost:9092"
TOPIC = "food-delivery-orders"
DATA_FILE = "sample_orders.csv"

# Delay between Kafka events
STREAM_DELAY = 0.5


# =========================================================
# ROBUST TIME PARSER
# Handles:
#   HH:MM
#   HH:MM:SS
#   Excel decimal time
#   Values greater than 24 hours
# =========================================================

def parse_time(value):

    value = str(value).strip()

    # Handle Excel-style decimal time
    if ":" not in value:

        fraction = float(value)

        return timedelta(
            days=fraction
        )

    # Handle HH:MM or HH:MM:SS

    parts = value.split(":")

    hours = int(float(parts[0]))
    minutes = int(float(parts[1]))

    seconds = 0

    if len(parts) >= 3:
        seconds = int(float(parts[2]))

    return timedelta(
        hours=hours,
        minutes=minutes,
        seconds=seconds
    )


# =========================================================
# LOAD DATASET
# =========================================================

print("Loading sample order data...")

df = pd.read_csv(DATA_FILE)

print(f"Loaded {len(df)} orders.")

print(
    f"Preparing simulated streaming events "
    f"for topic: {TOPIC}"
)


# =========================================================
# CREATE KAFKA PRODUCER
# =========================================================

producer = KafkaProducer(

    bootstrap_servers=[KAFKA_BROKER],

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),

    # Wait for acknowledgement from all available replicas
    acks="all",

    # Retry failed sends
    retries=5
)


# =========================================================
# VERIFY KAFKA CONNECTION
# =========================================================

try:

    producer.partitions_for(TOPIC)

    print(
        f"Connected successfully to Kafka "
        f"at {KAFKA_BROKER}"
    )

except KafkaError as e:

    print("Could not connect to Kafka.")

    print(f"Error: {e}")

    producer.close()

    raise SystemExit(1)


# =========================================================
# STREAM ORDER EVENTS
# =========================================================

total_events = 0


try:

    for _, row in df.iterrows():

        order_id = str(row["ID"])


        # -------------------------------------------------
        # ORDER DATE
        # -------------------------------------------------

        order_date = datetime.strptime(
            str(row["Order_Date"]).strip(),
            "%d-%m-%Y"
        ).date()


        # -------------------------------------------------
        # ORDER AND PICKUP TIMES
        # -------------------------------------------------

        order_time = parse_time(
            row["Time_Orderd"]
        )

        pickup_time = parse_time(
            row["Time_Order_picked"]
        )


        # -------------------------------------------------
        # CREATE TIMESTAMPS
        # -------------------------------------------------

        base_date = datetime.combine(
            order_date,
            datetime.min.time()
        )

        order_timestamp = (
            base_date + order_time
        )

        pickup_timestamp = (
            base_date + pickup_time
        )


        # -------------------------------------------------
        # DELIVERY TIMESTAMP
        # -------------------------------------------------

        delivery_timestamp = (
            pickup_timestamp
            + timedelta(
                minutes=int(
                    row["Time_taken (min)"]
                )
            )
        )


        # =================================================
        # COMMON ORDER INFORMATION
        # =================================================

        base_event = {

            "order_id":
                order_id,

            "delivery_person_id":
                str(
                    row["Delivery_person_ID"]
                ),

            "delivery_person_age":
                int(
                    row["Delivery_person_Age"]
                ),

            "delivery_person_rating":
                float(
                    row["Delivery_person_Ratings"]
                ),

            "city":
                str(
                    row["City"]
                ),

            "weather_conditions":
                str(
                    row["Weather_conditions"]
                ),

            "road_traffic_density":
                str(
                    row["Road_traffic_density"]
                ),

            "vehicle_condition":
                int(
                    row["Vehicle_condition"]
                ),

            "order_type":
                str(
                    row["Type_of_order"]
                ),

            "vehicle_type":
                str(
                    row["Type_of_vehicle"]
                ),

            "multiple_deliveries":
                int(
                    row["multiple_deliveries"]
                ),

            "festival":
                str(
                    row["Festival"]
                ),

            "time_taken_min":
                int(
                    row["Time_taken (min)"]
                )
        }


        # =================================================
        # SIMULATED ORDER LIFECYCLE
        # =================================================

        events = [

            {
                **base_event,

                "event_type":
                    "ORDER_PLACED",

                "event_timestamp":
                    order_timestamp.isoformat()
            },


            {
                **base_event,

                "event_type":
                    "ORDER_PICKED_UP",

                "event_timestamp":
                    pickup_timestamp.isoformat()
            },


            {
                **base_event,

                "event_type":
                    "ORDER_DELIVERED",

                "event_timestamp":
                    delivery_timestamp.isoformat()
            }

        ]


        # =================================================
        # SEND EVENTS TO KAFKA
        # =================================================

        for event in events:

            future = producer.send(
                TOPIC,
                value=event
            )


            try:

                metadata = future.get(
                    timeout=10
                )

                total_events += 1


                print(

                    f"[{total_events}] "

                    f"{event['event_type']} | "

                    f"Order: "
                    f"{event['order_id']} | "

                    f"City: "
                    f"{event['city']} | "

                    f"Delivery time: "
                    f"{event['time_taken_min']} min | "

                    f"Partition: "
                    f"{metadata.partition} | "

                    f"Offset: "
                    f"{metadata.offset}"

                )


            except KafkaError as e:

                print(

                    f"Failed to send event "
                    f"for order "
                    f"{event['order_id']}: "
                    f"{e}"

                )


            # Simulate continuous streaming
            time.sleep(
                STREAM_DELAY
            )


# =========================================================
# CLEANUP
# =========================================================

finally:

    producer.flush()

    producer.close()


    print()

    print(
        "========================================"
    )

    print(
        "Streaming completed."
    )

    print(
        f"Total events sent: "
        f"{total_events}"
    )

    print(
        f"Kafka topic: {TOPIC}"
    )

    print(
        "========================================"
    )