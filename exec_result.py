
class ExecutionStatus:

    QUEUED = 0
    STARTED = 1
    SUCCESS = 2
    SKIPPED = 3
    ERROR = 4
    CANCELLED = 5

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

    def __init__(self, runnable, run_group, status, exec_id=None, reason=None, inputs=None, output=None):
        self.exec_id = uuid.uuid4() if exec_id is None else exec_id
        self.exec_id = run_group
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

    def __setattr__(self, key, value):
        raise RuntimeError("ExecutionStatus cannot be modified directly; use get_new_status() to create a new object")

    def get_new_status(self, status=DEFAULT, reason=DEFAULT, output=DEFAULT):
        tk = exec_id.set(self.exec_id)
        new_status = ExecutionStatus(
            runnable=self.runnable,
            status=self.status if status is DEFAULT else self.check_valid_status(status),
            reason=self.reason if reason is DEFAULT else reason,
            inputs=self.inputs,
            output=self.output if output is DEFAULT else output,
        )
        exec_id.reset(tk)
        return new_status


class ExecutionResult:

    def __init__(self, status=ExecutionStatus.SUCCESS, reason=None, value=None):

        if value is None and status is ExecutionStatus.SUCCESS:
            raise ValueError("execution result cannot be None for a SUCCESS status")

        self.status = ExecutionStatus.check_valid_status(status)
        self.reason = reason
        self.output = value