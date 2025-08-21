# Kafka + domain configuration & traffic knobs

KAFKA_BOOTSTRAP = "kafka-service:9092"

TOPICS = {
    "pos": "pos-events",
    "staff": "staff-shifts",
    "security": "security-events",
    "inventory": "inventory-counts",
}

# domain
STORES = [f"store-{i:03d}" for i in range(1, 6)]  # 5 stores
CASHIERS_PER_STORE = 6
CASHIERS = {s: [f"{s}-cashier-{i:02d}" for i in range(1, CASHIERS_PER_STORE + 1)] for s in STORES}
MANAGERS = {s: f"{s}-manager" for s in STORES}

# catalog (sku -> price)
SKU_CATALOG = {
    "sku-001": 9.99,
    "sku-002": 19.49,
    "sku-003": 29.99,
    "sku-004": 59.00,
    "sku-005": 199.00,   # high value
    "sku-006": 349.00,   # high value
    "sku-007": 4.99,
    "sku-008": 12.49,
    "sku-009": 79.99,
    "sku-010": 149.00,
}

GEN_RATES = {
    "pos_sales_per_sec": 4,          # average sales per second (cluster-wide)
    "refund_probability": 0.002,      # % of sales that become refunds (normal)
    "refund_without_sale_probability": 0.0005,  # anomalous refunds
    "burst_refund_probability": 0.002,          # chance to trigger a refund burst pattern
    "burst_refund_count": 4,                      # burst size (>= threshold)
    "high_sale_gate_prob": 0.0005,    # % of high-value sales that trigger a nearby gate event
    "inventory_snapshot_interval_sec": 20,  # each store emits inventory snapshot roughly this often
    "inventory_shrink_prob": 0.0005,  # % of snapshots with shrinkage anomaly
}

THRESHOLDS = {
    "refund_amount": 100.0,     # high-value refund threshold
    "refund_count": 3,          # count in window
    "gate_window_ms": 300_000,  # 5 min
    "gate_amount": 200.0,
    "shrink_threshold": 5,
    "sale_ttl_ms": 86_400_000,
}

# misc
SECURITY_GATES = [f"gate-{i}" for i in range(1, 6)]
