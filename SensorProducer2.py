from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import sys

# Отримуємо sensor_id з аргументів командного рядка або генеруємо
sensor_id = int(sys.argv[1]) if len(sys.argv) > 1 else random.randint(1000, 9999)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(25, 45), 2),
        "humidity": round(random.uniform(15, 85), 2)
    }

    producer.send('building_sensors_olesia', value=data)
    print(f"Sent: {data}")
    time.sleep(2)

