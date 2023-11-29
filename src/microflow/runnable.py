import asyncio
import typing
from typing import Callable, Union
import uuid
import functools

from microflow.run_queue import ConcurrencyGroup
from microflow.run_strategy import RunStrategy
from microflow.run import (
    Run,
    ExecutionStatus,
    ExecutionResult,
)
from microflow.runner import BaseRunner
from microflow.utils import run_ctx

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
        run_strategy: Union[None, str] = RunStrategy.ANY_SUCCESS_NO_ERROR,
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

        parent_run = run_ctx.get()
        run = Run(
            self.name,
            run_id,
            parent_run.run_stack if parent_run else [],
            args,
            self.flow.event_store,
        )
        parent_run_token = run_ctx.set(run)

        if run.run_key is not None:
            cached_output = await self.flow.event_store.get_runs(
                runnable_name=self.name,
                status=ExecutionStatus.SUCCESS,
                run_key=run.run_key,
                limit=1,
            )
            if len(cached_output) > 0:
                run_ctx.reset(parent_run_token)
                return run.ended(
                    ExecutionResult(
                        status=ExecutionStatus.SKIPPED, value=cached_output[0].output
                    )
                )

        # check the run strategy to skip run if need be
        if not self.run_strategy.should_run(*args):
            run_ctx.reset(parent_run_token)
            return run.ended(None)

        # else, enqueue the job
        run.queued()

        res = None
        async with self.flow.queue.wait_in_queue(run.run_id, self.concurrency_groups):
            run.started()
            try:
                run_result = await self.runner.run(
                    self.name, run_id=run_id, run_args=args
                )
                if run_result["type"] in ["result", "error"]:
                    res = run.ended(run_result["result"])
                elif run_result["type"] == "terminated":
                    res = run.ended(
                        ExecutionResult(
                            status=ExecutionStatus.TERMINATED,
                        )
                    )
            except asyncio.CancelledError:
                run.cancelled()
            except Exception as e:
                res = run.ended(e)

        run_ctx.reset(parent_run_token)
        return res


class Task(Runnable):
    pass


class Manager(Runnable):
    pass


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

    def init(self):
        # translate inputs to Runnables
        for runnable in self._runnables.values():
            inputs = []
            if runnable.inputs is None:
                runnable.inputs = []
            for input in runnable.inputs:
                if isinstance(input, str):
                    inputs.append(self._runnables[input])
                elif isinstance(input, Runnable):
                    inputs.append(input)
                else:
                    raise ValueError(
                        "input can be only a Runnable or the name of a Runnable"
                    )
            runnable.inputs = inputs

        # check the absence of cycles
        def has_cycle(node: Runnable, names_path: list[str]):
            if node.name in names_path:
                return True
            if len(node.inputs) == 0:
                return False
            return any([has_cycle(input, names_path + [node.name]) for input in inputs])

        if any([has_cycle(runnable, []) for runnable in self._runnables.values()]):
            raise RuntimeError("cycle detected in Runnable inputs")

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
