import json
import time
import random
from kafka import KafkaProducer
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9092',  security_protocol="PLAINTEXT")

df = pd.read_csv("split_data/online.csv")
# read records as json
for record in df.to_dict(orient="records"):
    # print(record)
    future = producer.send(
        topic="health_data",
        value=json.dumps(record).encode("utf-8")
    )

    try:
        record_metadata = future.get(timeout=10)
        print(record_metadata)
    except Exception as e:
        print(f"Failed to send record: {e}")

    time.sleep(random.randint(500, 2000) / 1000.0)

producer.flush()