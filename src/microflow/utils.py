import asyncio
import threading
import contextvars


class EventLoopStore:
    def __init__(self):
        self._loop = None
        self._lock = threading.Lock()
        self._shutdown_future = None
        self.shutdown_event = threading.Event()

    def get_loop(self, start=False):
        with self._lock:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
            if self._loop is None and start:
                self._loop = asyncio.new_event_loop()
                self._shutdown_future = self._loop.create_future()
                self._loop.run_until_complete(self._shutdown_future)
            return self._loop

    def stop_loop(self):
        if self._shutdown_future:
            self._shutdown_future.set_result(True)


caller_type_ctx = contextvars.ContextVar("caller_type", default=None)
caller_name_ctx = contextvars.ContextVar("caller_name", default=None)
run_id_ctx = contextvars.ContextVar("run_id", default=None)
run_group_ctx = contextvars.ContextVar("run_group", default=None)
