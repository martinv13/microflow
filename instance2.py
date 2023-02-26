import threading


class Singleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance


class Instance(Singleton):

    def __init__(self):
        self._managers = {}
        self._tasks = {}
        self._loop = None

    def register_manager(self, manager):
        if manager.name in self._managers:
            raise ValueError(f"manager {manager.name} is already defined")
        self._managers[manager.name] = manager

    def register_task(self, task):
        if task.name in self._tasks:
            raise ValueError(f"task {task.name} is already defined")
        self._tasks[task.name] = task

    def register_loop(self, loop):
        self._loop = loop

    def get_loop(self):
        if self._loop is None:
            raise RuntimeError("loop was not set")
        return self._loop


class ExecutionResult:

    QUEUED = "QUEUED"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"

    def __init__(self, run_id, run_status, run_key=None, payload=None):
        self.run_id = run_id
        self.run_status = run_status if isinstance(run_status, list) else [run_status]
        self.run_key = run_key
        self.payload = payload


class ExecutionStore(Singleton):

    def __init__(self):
        self._lock = threading.Lock()
        self._runs_by_id = {}
        self._runs_by_key = {}

    def _get_runs_by_key(self, run_key):
        if run_key in self._runs_by_key and len(self._runs_by_key[run_key]) > 0:
            return list(map(self._runs_by_id.get, self._runs_by_key[run_key]))


class TaskExecutionStore(ExecutionStore):

    MISSING = "MISSING"
    MISSING_OR_ERROR = "MISSING_OR_ERROR"

    def register_run_by_key(self, run_key, run_on=MISSING_OR_ERROR):
        with self._lock:
            runs = self._get_runs_by_key(run_key)
            if not runs:
                pass

class ManagerExecutionStore(ExecutionStore):
    pass
