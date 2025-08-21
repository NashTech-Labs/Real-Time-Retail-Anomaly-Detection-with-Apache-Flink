import random
import time

from config import TOPICS, STORES, CASHIERS
from producer_utils import build_producer, safe_send


def now_ms():
    return int(time.time() * 1000)

def run_staff_stream(bootstrap: str, stop_flag):
    """
    Emits rolling shifts. Shifts overlap lightly so some events fall outside shifts
    (to exercise the 'outside shift' bonus risk).
    """
    prod = build_producer(bootstrap)
    # emit a new (short) shift for a random cashier every ~5s
    while not stop_flag.is_set():
        store = random.choice(STORES)
        cashier = random.choice(CASHIERS[store])

        start = now_ms() - random.randint(30_000, 90_000)   # sometimes already started
        end = start + random.randint(5, 15) * 60_000        # 5–15 minutes

        shift = {
            "employeeId": cashier,
            "storeId": store,
            "shiftStart": start,
            "shiftEnd": end,
            "role": "CASHIER"
        }
        safe_send(prod, TOPICS["staff"], key=cashier, value=shift)
        time.sleep(5)
