import asyncio
import contextvars
import uuid

from microflow.run_queue import RunQueue, ConcurrencyGroup
from microflow.run_strategy import RunStrategy
from microflow.exec_result import ExecutionStatus, ExecutionResult
from microflow.event_store import EventStore
from microflow.runner import Runner


caller_type_ctx = contextvars.ContextVar("caller_type", default=None)
caller_name_ctx = contextvars.ContextVar("caller_name", default=None)
run_id_ctx = contextvars.ContextVar("run_id", default=None)
run_group_ctx = contextvars.ContextVar("run_group", default=None)


class Runnable:
    def __init__(
        self,
        event_store: EventStore,
        run_queue: RunQueue,
        runner: Runner,
        fun=None,
        async_fun=None,
        name=None,
        inputs=None,
        schedule=None,
        max_concurrency=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        self.event_store = event_store
        self.run_queue = run_queue
        self.runner = runner
        self.fun = fun
        self.async_fun = async_fun
        if not callable(self.fun) and not callable(self.async_fun):
            raise ValueError("fun or async_fun must be provided as callable")
        if self.fun is not None and self.async_fun is not None:
            raise ValueError("cannot provide both fun and async_fun")
        self.name = fun.__name__ if name is None else name
        self.inputs = inputs
        self.schedule = schedule
        if max_concurrency is None:
            self.concurrency_groups = []
        elif isinstance(max_concurrency, ConcurrencyGroup):
            self.concurrency_groups = [max_concurrency]
        elif isinstance(max_concurrency, list) and all(
            map(lambda x: isinstance(x, ConcurrencyGroup), max_concurrency)
        ):
            self.concurrency_groups = max_concurrency
        elif isinstance(max_concurrency, int):
            self.concurrency_groups = ConcurrencyGroup(max_concurrency)
        else:
            raise ValueError("invalid max_concurrency")
        self.run_strategy = RunStrategy(runnable=self, run_strategy=run_strategy)
        if self.fun is not None:
            self.__module__ = self.fun.__module__
        else:
            self.__module__ = self.async_fun.__module__

    def __call__(self, *args) -> ExecutionStatus:
        fut = asyncio.run_coroutine_threadsafe(
            self.run_async(*args),
            asyncio.get_running_loop(),
        )
        return fut.result()

    async def run_async(self, *args) -> ExecutionStatus:
        # cannot call other tasks or managers from a task
        if caller_type_ctx.get() == "TASK":
            raise RuntimeError("cannot run other tasks or managers from within a task")

        # in case the runnable is called directly
        run_group = run_group_ctx.get()
        run_group_token = None
        if run_group_ctx.get() is None:
            run_group = uuid.uuid4()
            run_group_token = run_group_ctx.set(run_group)

        # check if the run_key already exists
        if len(args) > 0 and isinstance(args[0], dict):
            if "run_key" in args[0]:
                cached_output = await self.event_store.get_run_by_run_key(
                    self.name, args[0]["run_key"]
                )
                if cached_output is not None:
                    exec_status = ExecutionStatus(
                        runnable=self,
                        run_group=run_group,
                        status=ExecutionStatus.SKIPPED,
                        inputs=args,
                        output=cached_output,
                    )
                    self.event_store.log_event(exec_status)
                    return exec_status

        # check the run strategy to skip run if need be
        if not self.run_strategy.should_run(*args):
            exec_status = ExecutionStatus(
                runnable=self,
                run_group=run_group,
                status=ExecutionStatus.SKIPPED,
                inputs=args,
                output={},
            )
            self.event_store.log_event(exec_status)
            return exec_status

        # else, enqueue the job
        exec_status = ExecutionStatus(
            runnable=self,
            run_group=run_group,
            status=ExecutionStatus.QUEUED,
            inputs=args,
        )
        self.event_store.log_event(exec_status)

        async with self.run_queue.wait_in_queue(self):
            exec_status = exec_status.get_new_status(status=ExecutionStatus.STARTED)
            self.event_store.log_event(exec_status)
            try:
                run_result = await self.runner.run(
                    self, run_id=exec_status.exec_id, run_args=args
                )
                if run_result is None:
                    run_status = exec_status.get_new_status(
                        status=ExecutionStatus.SKIPPED,
                        output={},
                    )
                elif isinstance(run_result, dict):
                    run_status = exec_status.get_new_status(
                        status=ExecutionStatus.SUCCESS,
                        output=run_result,
                    )
                elif isinstance(run_result, ExecutionResult):
                    run_status = exec_status.get_new_status(
                        status=run_result.status,
                        output=run_result.output,
                    )
            except:
                run_status = exec_status.get_new_status(
                    status=ExecutionStatus.ERROR,
                    reason="execution failed",
                )
            self.event_store.log_event(run_status)
            if run_group_token is not None:
                run_group_ctx.reset(run_group_token)

    @classmethod
    def context_get(cls, variable, default=None):
        if variable == "run_id":
            return run_id_ctx.get()
        if variable == "run_group":
            return run_group_ctx.get()
        if variable == "caller_type":
            return caller_type_ctx.get()
        if variable == "caller_name":
            return caller_name_ctx.get()

    def __reduce__(self):
        return (
            self.fun.__qualname__
            if self.fun is not None
            else self.async_fun.__qualname__
        )


class Task(Runnable):
    async def run_async(self, *args):
        caller_type_ctx.set("TASK")
        return await super().run_async(self, *args)


class Manager(Runnable):
    async def run_async(self, *args):
        caller_type_ctx.set("MANAGER")
        return await super().run_async(self, *args)
