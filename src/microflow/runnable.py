import typing
from typing import Callable, Union
import uuid
import functools

from microflow.run_queue import ConcurrencyGroup
from microflow.run_strategy import RunStrategy
from microflow.exec_result import (
    ExecutionStatus,
    make_execution_result_from_output,
    ExecutionResult,
)
from microflow.runner import BaseRunner
from microflow.utils import run_group_ctx, caller_type_ctx

if typing.TYPE_CHECKING:
    from microflow.flow import Flow, Schedule


class Runnable:
    """A base class for tasks and managers, which share most API features"""

    def __init__(
        self,
        flow: "Flow",
        runner: BaseRunner,
        fun: Callable = None,
        name: str = None,
        inputs: Union[None, str, "Runnable", list[str], list["Runnable"]] = None,
        schedule: Union[None, str, "Schedule"] = None,
        max_concurrency: Union[None, int, "ConcurrencyGroup"] = None,
        max_concurrency_by_key: Union[None, str] = None,
        run_strategy: Union[None, str] =RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        self.flow = flow
        self.runner = runner
        self.fun = fun
        if not callable(self.fun):
            raise ValueError("fun must be provided as callable")
        self.name = name if name is not None else fun.__name__
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
        functools.update_wrapper(self, self.fun)

    def __call__(self, *args) -> ExecutionResult:
        """Run synchronously, either from within a running Flow (within a manager) or naked

        :param args:
        :return:
        """
        return self.flow.run_coro(self.run_async(*args))

    async def run_async(self, *args, run_id=None) -> ExecutionResult:
        """The main logic to run a manager or task

        :param run_id:
        :param args:
        :return:
        """
        if run_id is None:
            run_id = str(uuid.uuid4())

        # in case the runnable is called directly, set run_group context
        run_group = run_group_ctx.get()
        run_group_token = None
        if run_group is None:
            run_group = str(uuid.uuid4())
            run_group_token = run_group_ctx.set(run_group)

        run_args = {
            "run_id": run_id,
            "run_group": run_group,
            "runnable_name": self.name,
        }

        if len(args) > 0 and isinstance(args[0], dict):
            run_args["time_partition"] = (
                args[0]["time_partition"] if "time_partition" in args[0] else None
            )
            run_args["cat_partition"] = (
                args[0]["cat_partition"] if "cat_partition" in args[0] else None
            )

            # check if a successful run already exists for the given run_key
            if "run_key" in args[0]:
                run_args["run_key"] = args[0]["run_key"]
                cached_output = await self.flow.event_store.get_runs(
                    runnable_name=self.name,
                    status=ExecutionStatus.SUCCESS,
                    run_key=args[0]["run_key"],
                    limit=1,
                )
                if len(cached_output) > 0:
                    res = ExecutionResult(
                        status=ExecutionStatus.SKIPPED,
                        value=None,
                        run_key=run_args["run_key"],
                        time_partition=run_args["time_partition"],
                        cat_partition=run_args["cat_partition"],
                    )
                    res.runnable_name = self.name
                    res.run_id = run_id
                    res.run_group = run_group
                    res.output = cached_output[0].output
                    self.flow.event_store.log_event(
                        **run_args,
                        output=res.output,
                        status=ExecutionStatus.SKIPPED,
                        inputs=args,
                    )
                    return res

        # check the run strategy to skip run if need be
        if not self.run_strategy.should_run(*args):
            res = make_execution_result_from_output(**run_args, value=None)
            self.flow.event_store.log_event(
                **run_args, status=ExecutionStatus.SKIPPED, inputs=args
            )
            return res

        # else, enqueue the job
        self.flow.event_store.log_event(
            **run_args, status=ExecutionStatus.QUEUED, inputs=args
        )

        async with self.flow.queue.wait_in_queue(self.concurrency_groups):
            self.flow.event_store.log_event(**run_args, status=ExecutionStatus.STARTED)
            try:
                run_result = await self.runner.run(
                    self.name, run_id=run_id, run_args=args
                )
                if run_result["type"] in ["result", "error"]:
                    res = make_execution_result_from_output(
                        **run_args, value=run_result["result"]
                    )
            except Exception as e:
                res = make_execution_result_from_output(**run_args, value=e)
            self.flow.event_store.log_event(
                **run_args, status=res.status, output=res.output
            )
            if run_group_token is not None:
                run_group_ctx.reset(run_group_token)
            return res


class Task(Runnable):
    async def run_async(self, *args, run_id=None):
        # cannot call other tasks or managers from a task
        if caller_type_ctx.get() == "TASK":
            raise RuntimeError("cannot run other tasks or managers from within a task")
        caller_type_ctx.set("TASK")
        return await super().run_async(*args, run_id=run_id)


class Manager(Runnable):
    async def run_async(self, *args, run_id=None):
        caller_type_ctx.set("MANAGER")
        return await super().run_async(*args, run_id=run_id)


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

    def has_tasks(self):
        return any(
            [isinstance(runnable, Task) for runnable in self._runnables.values()]
        )

    def get_functions_as_dict(self, tasks_only: bool = False):
        return {
            key: val.fun
            for key, val in self._runnables.items()
            if not tasks_only or isinstance(val, Task)
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

    def keys(self):
        return self._runnables.keys()

    def values(self):
        return self._runnables.values()

    def items(self):
        return self._runnables.items()

    def copy(self):
        return self._runnables.copy()

    def get(self, arg):
        return self._runnables.get(arg)

    def __getitem__(self, item):
        return self._runnables.__getitem__(item)

    def __iter__(self):
        return self._runnables.__iter__()

    def __contains__(self, item):
        return self._runnables.__contains__(item)
