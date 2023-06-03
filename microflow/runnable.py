import asyncio
import contextvars
import typing
import uuid
import functools

from microflow.run_queue import ConcurrencyGroup
from microflow.run_strategy import RunStrategy
from microflow.exec_result import ExecutionStatus, ExecutionResult
from microflow.runner import Runner

if typing.TYPE_CHECKING:
    from microflow.flow import Flow


caller_type_ctx = contextvars.ContextVar("caller_type", default=None)
caller_name_ctx = contextvars.ContextVar("caller_name", default=None)
run_id_ctx = contextvars.ContextVar("run_id", default=None)
run_group_ctx = contextvars.ContextVar("run_group", default=None)


class Runnable:
    def __init__(
        self,
        flow: "Flow",
        runner: Runner,
        fun=None,
        async_fun=None,
        name=None,
        inputs=None,
        schedule=None,
        max_concurrency=None,
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
        self.name = (
            name
            if name is not None
            else fun.__name__
            if fun is not None
            else async_fun.__name__
        )
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
        functools.update_wrapper(
            self, self.fun if self.async_fun is None else self.async_fun
        )

    def __call__(self, *args) -> ExecutionStatus:
        return self.flow.run_coro(self.run_async(*args))

    async def run_async(self, *args, exec_id=None) -> ExecutionStatus:

        # in case the runnable is called directly
        run_group = run_group_ctx.get()
        run_group_token = None
        if run_group_ctx.get() is None:
            run_group = uuid.uuid4()
            run_group_token = run_group_ctx.set(run_group)

        # check if the run_key already exists
        if len(args) > 0 and isinstance(args[0], dict):
            if "run_key" in args[0]:
                cached_output = await self.flow.event_store.get_run_by_run_key(
                    self.name, args[0]["run_key"]
                )
                if cached_output is not None:
                    exec_status = ExecutionStatus(
                        runnable=self,
                        exec_id=exec_id,
                        run_group=run_group,
                        status=ExecutionStatus.SKIPPED,
                        inputs=args,
                        output=cached_output,
                    )
                    self.flow.event_store.log_event(exec_status)
                    return exec_status

        # check the run strategy to skip run if need be
        if not self.run_strategy.should_run(*args):
            exec_status = ExecutionStatus(
                runnable=self,
                exec_id=exec_id,
                run_group=run_group,
                status=ExecutionStatus.SKIPPED,
                inputs=args,
                output={},
            )
            self.flow.event_store.log_event(exec_status)
            return exec_status

        # else, enqueue the job
        exec_status = ExecutionStatus(
            runnable=self,
            exec_id=exec_id,
            run_group=run_group,
            status=ExecutionStatus.QUEUED,
            inputs=args,
        )
        self.flow.event_store.log_event(exec_status)

        async with self.flow.queue.wait_in_queue(self.concurrency_groups):
            exec_status = exec_status.get_new_status(status=ExecutionStatus.STARTED)
            self.flow.event_store.log_event(exec_status)
            try:
                run_result = await self.runner.run(
                    self, run_id=exec_status.exec_id, run_args=args
                )
                if run_result is None:
                    run_status = exec_status.get_new_status(
                        status=ExecutionStatus.SKIPPED,
                        output={},
                    )
                elif isinstance(run_result, ExecutionResult):
                    run_status = exec_status.get_new_status(
                        status=run_result.status,
                        reason=run_result.reason,
                        output=run_result.output,
                    )
                elif isinstance(run_result, dict):
                    run_status = exec_status.get_new_status(
                        status=ExecutionStatus.SUCCESS,
                        output=run_result,
                    )
            except Exception as e:
                run_status = exec_status.get_new_status(
                    status=ExecutionStatus.ERROR,
                    reason=f"execution failed: {e}",
                )
            self.flow.event_store.log_event(run_status)
            if run_group_token is not None:
                run_group_ctx.reset(run_group_token)
            return run_status

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


class Task(Runnable):
    async def run_async(self, *args, exec_id=None):
        # cannot call other tasks or managers from a task
        if caller_type_ctx.get() == "TASK":
            raise RuntimeError("cannot run other tasks or managers from within a task")
        caller_type_ctx.set("TASK")
        return await super().run_async(*args, exec_id=None)


class Manager(Runnable):
    async def run_async(self, *args, exec_id=None):
        caller_type_ctx.set("MANAGER")
        return await super().run_async(*args, exec_id=None)


class RunnableStore(dict):
    def __init__(self):
        super().__init__()
        self._runnables = {}

    def register(self, runnable: Runnable):
        if not isinstance(runnable, Task) and not isinstance(runnable, Manager):
            raise ValueError("runnable should be a Task or a Manager")
        if runnable.name in self._runnables:
            raise RuntimeError(f"runnable {runnable.name} is already defined")
        self._runnables[runnable.name] = runnable

    def get(self, runnable_name):
        return self._runnables[runnable_name]

    def has_tasks(self):
        return any(
            [isinstance(runnable, Task) for runnable in self._runnables.values()]
        )

    def get_tasks_functions_as_dict(self):
        return {
            key: val.fun if val.fun is not None else val.async_fun
            for key, val in self._runnables.items()
            if isinstance(val, Task)
        }

    def get_summary(self):
        return {
            "runnables": [
                {
                    "name": runnable.name,
                    "type": "task" if isinstance(runnable, Task) else "manager",
                }
                for runnable in self._runnables.values()
            ]
        }
