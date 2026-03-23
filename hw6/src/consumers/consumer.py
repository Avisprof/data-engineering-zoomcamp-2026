import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
TOPIC_NAME = 'green-trips'

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-console',
    value_deserializer=ride_deserializer,
    consumer_timeout_ms=15_000 # 15 seconds without new messages
)

print(f"Listening to {TOPIC_NAME}...")

count = 0
for message in consumer:
    ride = message.value
    if ride.trip_distance > 5:
        count += 1

print(f'There are {count} trips have "trip_distance" > 5')

consumer.close()