import random
import time
from collections import defaultdict, deque

class GeneratorState:
    """
    Tracks recent sales by (store,cashier,txnId) to permit refunds,
    and a rolling counter per store:sku for inventory/sales.
    """
    def __init__(self):
        # map txnId -> sale record
        self.sales_by_txn = {}
        # map (store:sku) -> sales since last snapshot
        self.sales_counters = defaultdict(int)
        # keep limited history of txns per cashier to seed bursts
        self.recent_txns_by_cashier = defaultdict(lambda: deque(maxlen=50))

    def record_sale(self, sale):
        txn = sale["transactionId"]
        self.sales_by_txn[txn] = sale
        key = f'{sale["storeId"]}:{sale["sku"]}'
        self.sales_counters[key] += 1
        self.recent_txns_by_cashier[(sale["storeId"], sale["cashierId"])].append(txn)

    def pop_sale(self, txn_id):
        return self.sales_by_txn.pop(txn_id, None)

    def pick_recent_txns(self, store_id, cashier_id, count):
        dq = self.recent_txns_by_cashier[(store_id, cashier_id)]
        return list(dq)[-count:] if dq else []

    def sales_since_snapshot(self, store_id, sku):
        return self.sales_counters.get(f"{store_id}:{sku}", 0)

    def reset_sales_since_snapshot(self, store_id, sku):
        self.sales_counters[f"{store_id}:{sku}"] = 0

STATE = GeneratorState()

def new_txn_id():
    return f"txn-{int(time.time()*1000)}-{random.randint(1000,9999)}"
