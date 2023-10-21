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
    MULTIPLE = "MULTIPLE"  #  useful when ExecutionResult is a list of ExecutionResults


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
        """ExecutionResult instance can be created by the user, but context arguments (run_id, etc.) will be set
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
        self.run_group = None
        self.timestamp = datetime.now()

        if status not in [
            ExecutionStatus.SKIPPED,
            ExecutionStatus.SUCCESS,
            ExecutionStatus.ERROR,
            ExecutionStatus.MULTIPLE,
        ]:
            raise ValueError("Invalid status")
        self.status = status

        self.output = value.output if isinstance(value, ExecutionResult) else value
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
            raise ValueError("Exception returned as output without error status")
        if status == ExecutionStatus.SKIPPED and self.output is not None:
            raise ValueError("Value must be None for SKIPPED status")
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
        return f"{self.timestamp}: {self.runnable_name} - {self.status} (group: {str(self.run_group)[-8:]})"


class Run(BaseModel):
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


def make_execution_result_from_output(
    value: Union[None, dict, "ExecutionResult", list, Exception],
    runnable_name: str,
    run_id: str,
    run_group: str,
    run_key: str = None,
    time_partition: datetime = None,
    cat_partition: str = None,
):
    if value is None:
        res = ExecutionResult(
            status=ExecutionStatus.SKIPPED,
            run_key=run_key,
            time_partition=time_partition,
            cat_partition=cat_partition,
        )
    elif isinstance(value, ExecutionResult):
        res = value
    elif isinstance(value, list) and any(
        isinstance(el, ExecutionResult) for el in value
    ):
        if not all(isinstance(el, ExecutionResult) for el in value):
            raise ValueError(
                "Mixed list of ExecutionResult and other types is not allowed"
            )
        res = ExecutionResult(
            status=ExecutionStatus.MULTIPLE,
            value=value,
            run_key=run_key,
            time_partition=time_partition,
            cat_partition=cat_partition,
        )
    elif isinstance(value, Exception):
        res = ExecutionResult(
            status=ExecutionStatus.ERROR,
            value=value,
            run_key=run_key,
            time_partition=time_partition,
            cat_partition=cat_partition,
        )
    else:
        res = ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            value=value,
            run_key=run_key,
            time_partition=time_partition,
            cat_partition=cat_partition,
        )

    res.runnable_name = runnable_name
    res.run_id = run_id
    res.run_group = run_group

    return res
