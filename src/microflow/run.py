from datetime import datetime
from pydantic import BaseModel
from typing import Any, Union
from enum import Enum

DEFAULT = object()


class ExecutionStatus(str, Enum):
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"
    QUEUED = "QUEUED"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    TERMINATED = "TERMINATED"
    MULTIPLE = "MULTIPLE"  # useful when ExecutionResult is a list of ExecutionResults


class ExecutionResult:
    """A class to represent the result of a run, which can be returned explicitly by a task or a manager
    decorated function.
    """

    def __init__(
        self,
        status: ExecutionStatus = ExecutionStatus.SUCCESS,
        value: Union[None, dict, "ExecutionResult", list, Exception] = None,
        run_key: str = None,
        time_partition: datetime = None,
        cat_partition: str = None,
    ):
        """ExecutionResult instance can be created by the user, but run_id will be set
        afterward.

        :param status: The run status
        :param value: A result value, which can be a dict, an ExecutionResult or a list
        :param run_key:
        :param time_partition:
        :param cat_partition:
        """
        super().__init__()
        self.runnable_name = None
        self.run_id = None
        self.timestamp = datetime.now()

        if status not in [
            ExecutionStatus.SKIPPED,
            ExecutionStatus.SUCCESS,
            ExecutionStatus.ERROR,
            ExecutionStatus.MULTIPLE,
        ]:
            raise ValueError("Invalid status")
        self.status = status

        self.output = value
        if isinstance(value, ExecutionResult):
            self.output = value.output

        if (
            self.output is not None
            and not isinstance(self.output, dict)
            and not isinstance(self.output, list)
            and not isinstance(self.output, Exception)
        ):
            raise ValueError(
                "Value must be None, a dict, an ExecutionResult, a list or an Exception"
            )
        if isinstance(self.output, Exception) and status is not ExecutionStatus.ERROR:
            raise ValueError(
                "Exception are only allowed for ExecutionStatus with error status"
            )
        if status == ExecutionStatus.MULTIPLE and (
            not isinstance(self.output, list)
            or not all([isinstance(v, ExecutionResult) for v in self.output])
        ):
            raise ValueError(
                "Value must be a list of ExecutionResult for MULTIPLE status"
            )

        self.run_key = run_key
        self.time_partition = time_partition
        self.cat_partition = cat_partition

    def keys(self):
        return self.output.keys()

    def values(self):
        return self.output.values()

    def items(self):
        return self.output.items()

    def copy(self):
        return self.output.copy()

    def get(self, arg):
        return self.output.get(arg)

    def __getitem__(self, item):
        return self.output.__getitem__(item)

    def __iter__(self):
        return self.output.__iter__()

    def __repr__(self):
        return f"{self.timestamp}: {self.runnable_name} - {self.status}"


class RunModel(BaseModel):
    """A Pydantic model for runs"""

    runnable_name: str
    run_id: str | None = None
    status: ExecutionStatus | None = None
    run_group: str | None = None
    session_id: str | None = None
    run_key: str | None = None
    time_partition: datetime | None = None
    cat_partition: str | None = None
    created_at: datetime | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    inputs: Union[tuple[dict], None] = None
    output: Any | None = None


class Run:
    """A run"""

    def __init__(self, runnable_name, run_id, run_stack, inputs, event_logger=None):
        self._event_logger = event_logger
        self._sub_runs = set()
        self.status = None
        self.output = None
        self.runnable_name = runnable_name
        self.run_id = run_id
        if isinstance(run_stack, list) and len(run_stack) > 0:
            run_stack[-1].register_sub_run(self)
        self.run_stack = (run_stack if isinstance(run_stack, list) else []) + [self]
        self.inputs = inputs
        self.run_key = None
        self.time_partition = None
        self.cat_partition = None
        if len(inputs) > 0 and isinstance(inputs[0], dict):
            for var in ["time_partition", "cat_partition", "run_key"]:
                if var in inputs[0]:
                    setattr(self, var, inputs[0][var])

    def _log_event(self):
        if self._event_logger:
            pass

    #            self._event_logger.log_event()

    def queued(self):
        self.status = ExecutionStatus.QUEUED
        self._log_event()

    def started(self):
        self.status = ExecutionStatus.STARTED
        self._log_event()

    def register_sub_run(self, sub_run: "Run"):
        self._sub_runs.add(sub_run)

    def cancelled(self):
        self.status = ExecutionStatus.CANCELLED
        self._log_event()
        for run in self._sub_runs:
            run.cancelled()

    def terminate(self):
        for run in self._sub_runs:
            run.terminate()

    def ended(self, output):
        if output is None:
            self.output = ExecutionResult(
                status=ExecutionStatus.SKIPPED,
                run_key=self.run_key,
                time_partition=self.time_partition,
                cat_partition=self.cat_partition,
            )
        elif isinstance(output, ExecutionResult):
            self.output = output
            for var in ["run_key", "time_partition", "cat_partition"]:
                if getattr(output, var) is not None:
                    if getattr(self, var) is not None:
                        raise ValueError(
                            f"{var} was already set and cannot be set again"
                        )
                    setattr(self, var, getattr(output, var))
                else:
                    setattr(output, var, getattr(self, var))
        elif isinstance(output, list) and any(
            isinstance(el, ExecutionResult) for el in output
        ):
            if not all(isinstance(el, ExecutionResult) for el in output):
                raise ValueError(
                    "Mixed list of ExecutionResult and other types is not allowed"
                )
            self.output = ExecutionResult(
                status=ExecutionStatus.MULTIPLE,
                value=output,
                run_key=self.run_key,
                time_partition=self.time_partition,
                cat_partition=self.cat_partition,
            )
        elif isinstance(output, Exception):
            self.output = ExecutionResult(
                status=ExecutionStatus.ERROR,
                value=output,
                run_key=self.run_key,
                time_partition=self.time_partition,
                cat_partition=self.cat_partition,
            )
        else:
            self.output = ExecutionResult(
                status=ExecutionStatus.SUCCESS,
                value=output,
                run_key=self.run_key,
                time_partition=self.time_partition,
                cat_partition=self.cat_partition,
            )
        self.output.run_id = self.run_id
        return self.output
