import random
import time
from datetime import datetime, timezone

from config import TOPICS, SKU_CATALOG, STORES, CASHIERS, GEN_RATES, THRESHOLDS
from producer_utils import build_producer, safe_send
from state import STATE, new_txn_id
from trigger_bus import BUS

def now_ms():
    return int(time.time() * 1000)

def random_store_cashier():
    store = random.choice(STORES)
    cashier = random.choice(CASHIERS[store])
    return store, cashier

def pick_sku():
    return random.choice(list(SKU_CATALOG.keys()))

def generate_sale_record(store, cashier, sku=None, amount=None):
    if sku is None:
        sku = pick_sku()
    price = SKU_CATALOG[sku]
    amt = price if amount is None else amount
    return {
        "transactionId": new_txn_id(),
        "storeId": store,
        "cashierId": cashier,
        "sku": sku,
        "amount": round(float(amt), 2),
        "eventType": "SALE",
        "eventTime": now_ms(),
    }

def generate_refund_record(from_sale):
    return {
        "transactionId": from_sale["transactionId"],
        "storeId": from_sale["storeId"],
        "cashierId": from_sale["cashierId"],
        "sku": from_sale["sku"],
        "amount": from_sale["amount"],
        "eventType": "REFUND",
        "eventTime": now_ms(),
    }

def run_pos_stream(bootstrap: str, stop_flag):
    prod = build_producer(bootstrap)

    sales_rate = GEN_RATES["pos_sales_per_sec"]
    interval = 1.0 / max(1, sales_rate)

    refund_p = GEN_RATES["refund_probability"]
    refund_wo_sale_p = GEN_RATES["refund_without_sale_probability"]
    burst_p = GEN_RATES["burst_refund_probability"]
    burst_n = GEN_RATES["burst_refund_count"]
    high_sale_gate_p = GEN_RATES["high_sale_gate_prob"]

    while not stop_flag.is_set():
        # 1) regular sale
        store, cashier = random_store_cashier()
        sale = generate_sale_record(store, cashier)
        STATE.record_sale(sale)

        # chance to make it a high-value sale for gate-correlation
        if sale["amount"] < THRESHOLDS["gate_amount"] and random.random() < 0.25:
            # bump to high-value SKU
            sale["sku"] = random.choice(["sku-005", "sku-006"])
            sale["amount"] = SKU_CATALOG[sale["sku"]]

        safe_send(prod, TOPICS["pos"], key=sale["transactionId"], value=sale)

        # maybe publish high-value trigger
        if sale["amount"] >= THRESHOLDS["gate_amount"] and random.random() < high_sale_gate_p:
            BUS.publish_high_sale(store)

        # 2) normal refund (from an actual sale)
        if random.random() < refund_p:
            refund = generate_refund_record(sale)
            safe_send(prod, TOPICS["pos"], key=refund["transactionId"], value=refund)

        # 3) refund WITHOUT a sale (anomaly)
        if random.random() < refund_wo_sale_p:
            fake = sale.copy()
            fake["transactionId"] = new_txn_id()  # not in STATE
            refund = generate_refund_record(fake)
            safe_send(prod, TOPICS["pos"], key=refund["transactionId"], value=refund)

        # 4) refund BURST for a cashier
        if random.random() < burst_p:
            txns = STATE.pick_recent_txns(store, cashier, burst_n)
            # if not enough recent real sales, fabricate along with refunds
            needed = burst_n - len(txns)
            for _ in range(needed):
                s = generate_sale_record(store, cashier, sku=random.choice(["sku-005","sku-006"]))
                STATE.record_sale(s)
                safe_send(prod, TOPICS["pos"], key=s["transactionId"], value=s)
                txns.append(s["transactionId"])
            # now emit refunds for these txns (high-value)
            for txn_id in txns:
                s = STATE.sales_by_txn.get(txn_id)
                if s:
                    s["amount"] = max(s["amount"], THRESHOLDS["refund_amount"] + 50.0)
                    r = generate_refund_record(s)
                    safe_send(prod, TOPICS["pos"], key=r["transactionId"], value=r)
                    time.sleep(0.1)

        time.sleep(interval)
