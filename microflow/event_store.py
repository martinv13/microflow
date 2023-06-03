import asyncio

from microflow.exec_result import ExecutionStatus
from microflow.runnable import Task


class EventStoreAdapter:
    async def log_event(self, run_event: ExecutionStatus):
        pass

    async def get_runs(self):
        pass

    async def get_run(self, run_id):
        pass


class EventStoreMemoryAdapter(EventStoreAdapter):
    def __init__(self):
        self._run_ids = []
        self._store = {}
        self._runnable_index = {}

    async def log_event(self, run_event: ExecutionStatus):
        if run_event.exec_id not in self._store:
            self._run_ids.append(run_event.exec_id)
            run_status = {
                "type": "TASK" if isinstance(run_event.runnable, Task) else "MANAGER",
                "runnable_name": run_event.runnable.name,
                "run_id": run_event.exec_id,
                "run_group": run_event.run_group,
                "inputs": run_event.inputs,
                "status": run_event.status,
                "created_at": run_event.timestamp,
            }
            if run_event.runnable.name not in self._runnable_index:
                self._runnable_index[run_event.runnable.name] = {}
            self._runnable_index[run_event.runnable.name]["last_run"] = run_status
        else:
            run_status = self._store[run_event.exec_id]
        run_status["status"] = run_event.status
        for status in ["QUEUED", "STARTED", "CANCELLED", "SKIPPED", "SUCCESS", "ERROR"]:
            if run_event.status == getattr(ExecutionStatus, status):
                run_status[f"{status.lower()}_at"] = run_event.timestamp
                run_status[f"{status.lower()}_reason"] = run_event.reason
                break

        self._store[run_event.exec_id] = run_status

    async def get_runs(self):
        return [self._store[run_id] for run_id in self._run_ids]

    async def get_run(self, run_id):
        return self._store[run_id]


class EventStore:
    def __init__(self, event_store_adapter: EventStoreAdapter):
        self.store = event_store_adapter
        self._background_tasks = set()

    def log_event(self, run_event: ExecutionStatus):
        print(run_event)
        do_log = asyncio.create_task(self.store.log_event(run_event))
        self._background_tasks.add(do_log)
        do_log.add_done_callback(self._background_tasks.discard)

    async def get_run_by_run_key(self, runnable_name, run_key):
        return None

    async def get_runs(self):
        return await self.store.get_runs()
