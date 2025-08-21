import random
import time
from config import TOPICS, STORES, MANAGERS, SKU_CATALOG, GEN_RATES, THRESHOLDS
from producer_utils import build_producer, safe_send
from state import STATE

def now_ms():
    return int(time.time() * 1000)

def run_inventory_stream(bootstrap: str, stop_flag):
    """
    Every store periodically emits inventory counts for a few SKUs.
    Sometimes undercount to trigger shrinkage alerts.
    """
    prod = build_producer(bootstrap)
    interval = GEN_RATES["inventory_snapshot_interval_sec"]

    while not stop_flag.is_set():
        for store in STORES:
            manager = MANAGERS[store]
            # choose 3 random SKUs to snapshot
            for sku in random.sample(list(SKU_CATALOG.keys()), k=3):
                # how many sales since last snapshot?
                sales = STATE.sales_since_snapshot(store, sku)
                # base count close to sales (normal)
                counted = max(0, sales - random.randint(-2, 2))
                # sometimes force shrinkage
                if random.random() < GEN_RATES["inventory_shrink_prob"]:
                    counted = max(0, sales - (THRESHOLDS["shrink_threshold"] + random.randint(1, 4)))
                evt = {
                    "storeId": store,
                    "managerId": manager,
                    "sku": sku,
                    "countedQty": int(counted),
                    "eventTime": now_ms(),
                }
                safe_send(prod, TOPICS["inventory"], key=f"{store}:{sku}", value=evt)
                # reset sales counter post-snapshot
                STATE.reset_sales_since_snapshot(store, sku)
        # wait roughly the interval, but keep responsive to stop
        for _ in range(interval * 5):
            if stop_flag.is_set():
                break
            time.sleep(0.2)
