import threading
import signal
import time

from config import KAFKA_BOOTSTRAP
from pos_stream import run_pos_stream
from staff_stream import run_staff_stream
from security_stream import run_security_stream
from inventory_stream import run_inventory_stream

class StopFlag:
    def __init__(self):
        self._ev = threading.Event()
    def is_set(self):
        return self._ev.is_set()
    def set(self):
        self._ev.set()

def main():
    stop = StopFlag()

    def handle_sig(sig, frame):
        print("\n[INFO] stopping generators...")
        stop.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    threads = [
        threading.Thread(target=run_pos_stream, args=(KAFKA_BOOTSTRAP, stop), daemon=True),
        threading.Thread(target=run_staff_stream, args=(KAFKA_BOOTSTRAP, stop), daemon=True),
        threading.Thread(target=run_security_stream, args=(KAFKA_BOOTSTRAP, stop), daemon=True),
        threading.Thread(target=run_inventory_stream, args=(KAFKA_BOOTSTRAP, stop), daemon=True),
    ]
    for t in threads:
        t.start()

    print("[OK] generators running. Ctrl+C to stop.")
    while not stop.is_set():
        time.sleep(1)

if __name__ == "__main__":
    main()
