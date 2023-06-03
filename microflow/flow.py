import asyncio
import os
import functools
import uuid
from multiprocessing import cpu_count
import uvicorn

from microflow.utils import EventLoopStore
from microflow.runnable import RunStrategy, Task, Manager, RunnableStore, Runnable
from microflow.run_queue import RunQueue, ConcurrencyGroup
from microflow.event_store import EventStore, EventStoreMemoryAdapter
from microflow.runner import ThreadingRunner, ProcessPoolRunner
from microflow.api import create_app

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.basename(__file__), "../../../"))


class Schedule:
    def __init__(self, expr, name=None):
        self.schedule_expr = expr
        self.name = name


class Flow:
    def __init__(
        self,
        loop_store,
        runnable_store,
        queue,
        event_store,
        tasks_runner,
        managers_runner,
        global_task_concurrency,
        flow_name=None,
    ):
        self.flow_name = flow_name
        self._init = False
        self._init_fut = None
        self._background_tasks = set()

        self._global_tasks_concurrency = ConcurrencyGroup(
            global_task_concurrency, name="GLOBAL_TASKS_CONCURRENCY"
        )
        self.loop_store = loop_store
        self.runnables = runnable_store
        self.queue = queue
        self.event_store = event_store
        self._tasks_runner = tasks_runner
        self._managers_runner = managers_runner
        self.app = None

    def _runnable_decorator(
        self,
        func=None,
        *,
        name=None,
        inputs=None,
        max_concurrency=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
        runnable_type=None,
        async_mode=False,
    ):
        if func is None:
            return functools.partial(
                self._runnable_decorator,
                name=name,
                inputs=inputs,
                max_concurrency=max_concurrency,
                schedule=schedule,
                run_strategy=run_strategy,
                runnable_type=runnable_type,
                async_mode=async_mode,
            )
        async_fun = func if async_mode else None
        fun = None if async_mode else func
        if runnable_type == "TASK":
            runnable = Task(
                flow=self,
                runner=self._tasks_runner,
                fun=fun,
                async_fun=async_fun,
                name=name,
                inputs=inputs,
                max_concurrency=max_concurrency,
                schedule=schedule,
                run_strategy=run_strategy,
            )
            runnable.concurrency_groups.append(self._global_tasks_concurrency)
        else:
            runnable = Manager(
                flow=self,
                runner=self._managers_runner,
                fun=fun,
                async_fun=async_fun,
                name=name,
                inputs=inputs,
                schedule=schedule,
                run_strategy=run_strategy,
            )
        self.runnables.register(runnable)
        return runnable

    def task(
        self,
        func=None,
        *,
        name=None,
        inputs=None,
        max_concurrency=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        return self._runnable_decorator(
            func=func,
            name=name,
            inputs=inputs,
            max_concurrency=max_concurrency,
            schedule=schedule,
            run_strategy=run_strategy,
            runnable_type="TASK",
            async_mode=False,
        )

    def task_async(
        self,
        func=None,
        *,
        name=None,
        inputs=None,
        max_concurrency=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        return self._runnable_decorator(
            func=func,
            name=name,
            inputs=inputs,
            max_concurrency=max_concurrency,
            schedule=schedule,
            run_strategy=run_strategy,
            runnable_type="TASK",
            async_mode=True,
        )

    def manager(
        self,
        func=None,
        *,
        name=None,
        inputs=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        return self._runnable_decorator(
            func=func,
            name=name,
            inputs=inputs,
            schedule=schedule,
            run_strategy=run_strategy,
            runnable_type="MANAGER",
            async_mode=False,
        )

    def manager_async(
        self,
        func=None,
        *,
        name=None,
        inputs=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        return self._runnable_decorator(
            func=func,
            name=name,
            inputs=inputs,
            schedule=schedule,
            run_strategy=run_strategy,
            runnable_type="MANAGER",
            async_mode=True,
        )

    async def init(self):
        if self._init:
            return
        _loop = self.loop_store.get_loop()
        if _loop is None:
            loop = asyncio.new_event_loop()
            self._init_fut = loop.create_future()
            loop.run_forever()
            _loop = self.loop_store.get_loop()
        if self.runnables.has_tasks():
            await asyncio.gather(
                self._tasks_runner.start(),
                self._managers_runner.start(),
            )
        else:
            await self._managers_runner.start()
        self._init = True

    async def shutdown(self):
        await asyncio.gather(
            self._tasks_runner.shutdown(),
            self._managers_runner.shutdown(),
        )
        if self._init_fut:
            self._init_fut.set_result(True)

    def run_coro(self, coro):
        loop = self.loop_store.get_loop()
        if loop is not None:
            fut = asyncio.run_coroutine_threadsafe(coro, loop)
            return fut.result()

        async def init_run_shutdown():
            await self.init()
            res = await coro
            await self.shutdown()
            return res

        return asyncio.run(init_run_shutdown())

    def start_run(self, runnable, *args):
        if isinstance(runnable, Runnable):
            target = runnable
        else:
            target = self.runnables.get(runnable)
        exec_id = uuid.uuid4()
        asyncio.run_coroutine_threadsafe(target.run_async(*args, exec_id=exec_id), self.loop_store.get_loop())
        return exec_id

    def serve(self, host="localhost", port=3000):
        if self.app is None:
            self.app = create_app(self)
        uvicorn.run(self.app, host=host, port=port, log_level="info")


def create_flow(flow_name=None):
    loop_store = EventLoopStore()
    runnable_store = RunnableStore()
    cpus = cpu_count()
    return Flow(
        loop_store=loop_store,
        runnable_store=runnable_store,
        queue=RunQueue(),
        event_store=EventStore(EventStoreMemoryAdapter()),
        tasks_runner=ProcessPoolRunner(
            loop_store=loop_store, nb_workers=cpus, runnable_store=runnable_store
        ),
        managers_runner=ThreadingRunner(),
        global_task_concurrency=cpus,
        flow_name=flow_name,
    )
