from kafka import KafkaConsumer
import json

# Створення Consumer для обох топіків
consumer = KafkaConsumer(
    'temperature_alerts_olesia', 'humidity_alerts_olesia',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='alerts_viewer_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📡 Listening to alerts...\n")

for message in consumer:
    print(f"🔔 Alert received from topic [{message.topic}]: {message.value}")
