import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

def build_producer(bootstrap_servers: str) -> KafkaProducer:
    # linger/batch to lower overhead under high TPS
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=10,
        linger_ms=20,
        batch_size=32_768,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: (v.encode("utf-8") if isinstance(v, str) else v),
    )

def safe_send(producer: KafkaProducer, topic: str, key: str | None, value: dict):
    try:
        fut = producer.send(topic, key=key, value=value)
        # fire-and-forget is fine; but calling get() sporadically ensures backpressure
        if int(time.time() * 10) % 50 == 0:
            fut.get(timeout=5)
    except KafkaError as e:
        # log to stdout for simplicity
        print(f"[WARN] send failed topic={topic} error={e}")
