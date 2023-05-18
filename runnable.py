import asyncio
import contextvars
import uuid

from microflow.run_queue import ConcurrencyGroup
from microflow.run_strategy import RunStrategy
from microflow.exec_result import ExecutionStatus


caller_type = contextvars.ContextVar("caller_type", default=None)
caller_name = contextvars.ContextVar("caller_name", default=None)
run_id = contextvars.ContextVar("run_id", default=None)
run_group = contextvars.ContextVar("run_group", default=None)


class Runnable:
    def __init__(
        self,
        flow,
        runner,
        fun=None,
        async_fun=None,
        name=None,
        inputs=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        self.flow = flow
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
        self.run_strategy = RunStrategy(self, run_strategy, flow.event_store)
        if self.fun is not None:
            self.__module__ = self.fun.__module__
        else:
            self.__module__ = self.async_fun.__module__

    def __call__(self, *args, **kwargs):
        fut = asyncio.run_coroutine_threadsafe(
            self.run_async(*args),
            asyncio.get_running_loop(),
        )
        return fut.result()

    async def run_async(self, *args):

        if caller_type.get() == "TASK":
            raise RuntimeError("cannot run other tasks or managers from within a task")

        if run_group.get() is None:
            run_group_token = run_group.set(uuid.uuid4())

        # check the run strategy (run_key and input statuses) to skip run if need be
        exec_status = self.run_strategy.should_run(*args)
        self.flow.event_store.log_event(exec_status)

        if exec_status.status is ExecutionStatus.QUEUED:
            async with self.flow.queue.wait_in_queue(self):
                exec_status = exec_status.get_new_status(status=ExecutionStatus.STARTED)
                self.flow.event_store.log_event(exec_status)
                run_handle = self.runner.start_run(self, *args)
                self._exec_handles[exec_status.run_id] = run_handle
                result = await run_handle.result()

                self._event_logger.log(exec)

        run_group.reset(run_group_token)

    @classmethod
    def context_get(cls, variable, default=None):
        if variable == "run_id":
            return run_id.get()
        if variable == "run_group":
            return run_group.get()
        if variable == "caller_type":
            return caller_type.get()
        if variable == "caller_name":
            return caller_name.get()

    def __reduce__(self):
        return (
            self.fun.__qualname__
            if self.fun is not None
            else self.async_fun.__qualname__
        )


class Task(Runnable):
    def __init__(
        self,
        runner,
        fun=None,
        async_fun=None,
        name=None,
        inputs=None,
        max_concurrency=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        super().__init__(runner, fun, async_fun, name, inputs, schedule, run_strategy)
        self.concurrency_group = (
            None
            if max_concurrency is None
            else max_concurrency
            if isinstance(max_concurrency, ConcurrencyGroup)
            else ConcurrencyGroup(max_concurrency)
        )

    async def run_async(self, *args):
        caller_type.set("TASK")
        return await super().run_async(self, *args)


class Manager(Runnable):
    def __init__(
        self,
        runner,
        fun=None,
        async_fun=None,
        name=None,
        inputs=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        super().__init__(runner, fun, async_fun, name, inputs, schedule, run_strategy)

    async def run_async(self, *args):
        caller_type.set("MANAGER")
        return await super().run_async(self, *args)
