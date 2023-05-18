import asyncio
import os
import functools
from multiprocessing import cpu_count

from microflow.app import make_sanic_app
from microflow.runnable import ExecutionStatus, RunStrategy, Task, Manager
from microflow.control import run_parallel, run_with_dependencies_async, map_parallel
from microflow.run_queue import RunQueue, ConcurrencyGroup
from microflow.event_store import EventStore
from microflow.runner import InProcessRunner, MultiprocessingRunner


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.basename(__file__), "../../"))


class Schedule:
    def __init__(self, expr, name=None):
        self.schedule_expr = expr
        self.name = name


class Flow:
    def __init__(self, flow_name):
        self.flow_name = flow_name
        self.managers = {}
        self.tasks = {}
        self.sanic_app = None

        self._global_tasks_concurrency = ConcurrencyGroup(cpu_count(), name="GLOBAL_TASKS_CONCURRENCY")
        self._queue = RunQueue()
        self._event_store = EventStore()
        self._tasks_runner = MultiprocessingRunner(self)
        self._managers_runner = InProcessRunner(self)

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
        dest_dict = self.tasks if runnable_type == "task" else self.managers
        runnable = (
            Task(
                instance=self,
                fun=fun,
                async_fun=async_fun,
                name=name,
                inputs=inputs,
                max_concurrency=max_concurrency,
                schedule=schedule,
                run_strategy=run_strategy,
            )
            if runnable_type == "task"
            else Manager(
                instance=self,
                fun=func,
                async_fun=async_fun,
                name=name,
                inputs=inputs,
                schedule=schedule,
                run_strategy=run_strategy,
            )
        )
        if runnable.name in dest_dict:
            raise RuntimeError(f"{runnable_type} {runnable.name} is already defined")
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
            runnable_type="task",
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
            runnable_type="task",
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
            runnable_type="manager",
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
            runnable_type="manager",
            async_mode=True,
        )

    async def run_task(self, task_obj, args):
        exec = ExecutionStatus(task_obj, ExecutionStatus.QUEUED, inputs=args)
        self._event_logger.log(exec)
        async with self.tasks_queue.wait_in_queue(task_obj):
            exec.status = ExecutionStatus.STARTED
            self._event_logger.log(exec)
            exec = await self._runner.run(exec)
            self._event_logger.log(exec)
        return exec

    def run_parallel(self, *args):
        pass

    def run_managers(self, steps):
        fut = asyncio.run_coroutine_threadsafe(
            run_with_dependencies_async(steps),
            self.loop,
        )
        return fut.result()

    def run_server(self, port=3000, host="localhost"):
        if self.sanic_app is None:
            self.sanic_app = make_sanic_app(self)

        self.sanic_app.run(host=host, port=port)
