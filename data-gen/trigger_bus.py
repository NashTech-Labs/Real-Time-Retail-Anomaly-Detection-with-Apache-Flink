import queue

class TriggerBus:
    """
    Simple in-process bus: pos_stream pushes 'high-value sale' triggers,
    security_stream consumes them to emit near-in-time gate events.
    """
    def __init__(self):
        self._q = queue.Queue(maxsize=10_000)

    def publish_high_sale(self, store_id: str):
        try:
            self._q.put_nowait({"storeId": store_id})
        except queue.Full:
            pass

    def poll_high_sale(self, timeout=0.2):
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

BUS = TriggerBus()
