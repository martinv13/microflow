import asyncio
import threading


class EventLoopStore:
    def __init__(self, callback=None):
        self._loop = None
        self._callback = callback
        self._lock = threading.Lock()

    def get_loop(self):
        with self._lock:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
            return self._loop
