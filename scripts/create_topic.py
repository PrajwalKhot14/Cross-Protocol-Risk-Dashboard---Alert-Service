import os
from dotenv import load_dotenv
load_dotenv("env/.env")

brokers = os.getenv("KAFKA_BROKERS")
print("→ Using brokers:", brokers)
from kafka.admin import KafkaAdminClient, NewTopic


admin = KafkaAdminClient(
    bootstrap_servers=os.getenv("KAFKA_BROKERS").split(","),
)
topic = "aave-raw"

if topic not in admin.list_topics():
    admin.create_topics([
        NewTopic(name=topic, num_partitions=3, replication_factor=1)
    ])
    print(f"🆕 Created topic {topic}")
else:
    print(f"ℹ️ Topic {topic} already exists")

admin.close()
