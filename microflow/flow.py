import asyncio
import os
import functools
from multiprocessing import cpu_count

from microflow.runnable import RunStrategy, Task, Manager
from microflow.run_queue import RunQueue, ConcurrencyGroup
from microflow.event_store import EventStore
from microflow.runner import ThreadingRunner, MultiprocessingRunner


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.basename(__file__), "../../../"))


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

        self._global_tasks_concurrency = ConcurrencyGroup(
            cpu_count(), name="GLOBAL_TASKS_CONCURRENCY"
        )
        self._queue = RunQueue()
        self._event_store = EventStore()
        self._tasks_runner = MultiprocessingRunner()
        self._managers_runner = ThreadingRunner()

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
        dest_dict = self.tasks if runnable_type == "TASK" else self.managers
        if runnable_type == "TASK":
            runnable = Task(
                event_store=self._event_store,
                run_queue=self._queue,
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
                event_store=self._event_store,
                run_queue=self._queue,
                runner=self._tasks_runner,
                fun=func,
                async_fun=async_fun,
                name=name,
                inputs=inputs,
                schedule=schedule,
                run_strategy=run_strategy,
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
