import uuid
from datetime import datetime

DEFAULT = object()


class BaseExecutionStatus(dict):

    QUEUED = "QUEUED"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"

    @classmethod
    def check_valid_status(cls, status):
        if status not in [
            cls.QUEUED,
            cls.STARTED,
            cls.SUCCESS,
            cls.SKIPPED,
            cls.ERROR,
            cls.CANCELLED,
        ]:
            raise ValueError("invalid status value")
        return status


class ExecutionStatus(BaseExecutionStatus):
    def __init__(
        self,
        runnable,
        run_group,
        status,
        exec_id=None,
        reason=None,
        inputs=None,
        output=None,
    ):
        super().__init__()
        self.timestamp = datetime.now()
        self.exec_id = uuid.uuid4() if exec_id is None else exec_id
        self.run_group = run_group
        self.runnable = runnable
        self.status = ExecutionStatus.check_valid_status(status)
        self.reason = reason
        self.inputs = inputs
        if output is None:
            output = {}
        if not isinstance(output, dict):
            raise ValueError("execution output must be None or a dict")
        self.output = output

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
        return f"{self.timestamp}: {self.runnable.name} - {self.status} (group: {str(self.run_group)[-8:]})"

    def get_new_status(self, status=DEFAULT, reason=DEFAULT, output=DEFAULT):
        new_status = ExecutionStatus(
            runnable=self.runnable,
            status=self.status
            if status is DEFAULT
            else self.check_valid_status(status),
            run_group=self.run_group,
            exec_id=self.exec_id,
            reason=self.reason if reason is DEFAULT else reason,
            inputs=self.inputs,
            output=self.output if output is DEFAULT else output,
        )
        return new_status


class ExecutionResult(BaseExecutionStatus):
    def __init__(self, status=ExecutionStatus.SUCCESS, reason=None, value=None):
        super().__init__()
        if value is None and status is ExecutionStatus.SUCCESS:
            raise ValueError("execution result cannot be None for a SUCCESS status")

        self.status = ExecutionStatus.check_valid_status(status)
        self.reason = reason
        self.output = value
