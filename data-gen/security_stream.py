import random
import time
from config import TOPICS, SECURITY_GATES
from producer_utils import build_producer, safe_send
from trigger_bus import BUS

def now_ms():
    return int(time.time() * 1000)

def run_security_stream(bootstrap: str, stop_flag):
    """
    Consumes high-value sale triggers; emits a gate event shortly after.
    Also emits occasional background security noise.
    """
    prod = build_producer(bootstrap)

    noise_interval = 7.0
    next_noise = time.time() + noise_interval

    while not stop_flag.is_set():
        trig = BUS.poll_high_sale(timeout=0.3)
        if trig:
            evt = {
                "eventId": f"sec-{now_ms()}",
                "storeId": trig["storeId"],
                "eventTime": now_ms(),
                "eventType": "GATE",
                "eventData": {
                    "gateId": random.choice(SECURITY_GATES),
                    "alarm": True
                }
            }
            safe_send(prod, TOPICS["security"], key=evt["eventId"], value=evt)

        # background noise
        if time.time() >= next_noise:
            evt = {
                "eventId": f"sec-{now_ms()}",
                "storeId": trig["storeId"] if trig else f"store-{random.randint(1,5):03d}",
                "eventTime": now_ms(),
                "eventType": "DOOR_OPEN",
                "eventData": {
                    "gateId": random.choice(SECURITY_GATES),
                    "alarm": bool(random.random() < 0.1)
                }
            }
            safe_send(prod, TOPICS["security"], key=evt["eventId"], value=evt)
            next_noise = time.time() + noise_interval
