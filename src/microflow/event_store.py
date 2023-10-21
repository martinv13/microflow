import asyncio
import datetime
import uuid
from abc import ABC, abstractmethod
from typing import Any

from microflow.exec_result import ExecutionStatus, Run


class EventStoreAdapter(ABC):
    @abstractmethod
    async def log_event(
        self,
        session_id: str,
        runnable_name: str,
        run_group: str,
        run_id: str,
        status: str,
        timestamp: datetime.datetime,
        run_key: str = None,
        time_partition: datetime.datetime = None,
        cat_partition: str = None,
        inputs: tuple = None,
        output: Any = None,
    ):
        pass

    @abstractmethod
    async def get_run(self, run_id: str):
        pass

    @abstractmethod
    async def get_runs(
        self,
        runnable_name: str = None,
        status: str = None,
        run_group: str = None,
        run_key: str = None,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
        limit: int = None,
    ):
        pass


class EventStoreMemoryAdapter(EventStoreAdapter):
    def __init__(self):
        self._run_ids = []
        self._store = {}
        self._index_by_name = {}
        self._index_by_run_key = {}

    async def log_event(
        self,
        session_id: str,
        runnable_name: str,
        run_group: str,
        run_id: str,
        status: str,
        timestamp: datetime.datetime,
        run_key: str = None,
        time_partition: datetime.datetime = None,
        cat_partition: str = None,
        inputs: tuple = None,
        output: Any = None,
    ):
        if run_id not in self._store:
            self._run_ids.append(run_id)
            run = Run(
                session_id=session_id,
                runnable_name=runnable_name,
                run_id=run_id,
                run_group=run_group,
                inputs=inputs if len(inputs) > 0 else None,
                output=output,
                status=status,
                created_at=timestamp,
            )
            self._store[run_id] = run
            if runnable_name not in self._index_by_name:
                self._index_by_name[runnable_name] = {}
            self._index_by_name[runnable_name]["last_run"] = run
        else:
            run = self._store[run_id]
        run.status = status
        if status == ExecutionStatus.QUEUED:
            run.queued_at = timestamp
        elif status == ExecutionStatus.STARTED:
            run.started_at = timestamp
        else:
            run.ended_at = timestamp
        if run_key is not None:
            run.run_key = run_key
            self._index_by_run_key[(runnable_name, run_key)] = run
        if output is not None:
            run.output = output
        if inputs is not None:
            run.inputs = inputs if len(inputs) > 0 else None
        if time_partition is not None:
            run.time_partition = time_partition
        if cat_partition is not None:
            run.cat_partition = cat_partition

    async def get_runs(self, **kwargs) -> list[Run]:
        res = [self._store[run_id] for run_id in self._run_ids]
        if (
            "run_key" in kwargs
            and kwargs["run_key"] is not None
            and "runnable_name" in kwargs
            and kwargs["runnable_name"] is not None
        ):
            if (kwargs["runnable_name"], kwargs["run_key"]) in self._index_by_run_key:
                res = [
                    self._index_by_run_key[(kwargs["runnable_name"], kwargs["run_key"])]
                ]
        if "status" in kwargs and kwargs["status"] is not None:
            res = [r for r in res if r.status == kwargs["status"]]
        if (
            "limit" in kwargs
            and kwargs["limit"] is not None
            and len(res) > kwargs["limit"]
        ):
            return res[-kwargs["limit"] :]
        return res

    async def get_run(self, run_id: str) -> Run:
        return self._store[run_id]


class EventStore:
    def __init__(self, event_store_adapter: EventStoreAdapter):
        self.store = event_store_adapter
        self.session_id = str(uuid.uuid4())
        self._background_tasks = set()

    def log_event(
        self,
        runnable_name: str,
        run_id: str,
        status: ExecutionStatus,
        run_group: str,
        run_key: str = None,
        time_partition: datetime = None,
        cat_partition: str = None,
        inputs: tuple = None,
        output: Any = None,
    ):
        timestamp = datetime.datetime.now()
        do_log = asyncio.create_task(
            self.store.log_event(
                session_id=self.session_id,
                runnable_name=runnable_name,
                run_id=run_id,
                status=status,
                timestamp=timestamp,
                run_group=run_group,
                run_key=run_key,
                time_partition=time_partition,
                cat_partition=cat_partition,
                inputs=inputs,
                output=output,
            )
        )
        self._background_tasks.add(do_log)
        do_log.add_done_callback(self._background_tasks.discard)

    async def get_run(self, run_id: str) -> Run:
        return await self.store.get_run(run_id)

    async def get_runs(
        self,
        runnable_name: str = None,
        status: str = None,
        run_group: str = None,
        run_key: str = None,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
        limit: int = None,
    ) -> list[Run]:
        return await self.store.get_runs(
            runnable_name=runnable_name,
            status=status,
            run_group=run_group,
            run_key=run_key,
            date_start=date_start,
            date_end=date_end,
            limit=limit,
        )
