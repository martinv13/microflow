from microflow.exec_result import ExecutionStatus


class EventStore:
    def log_event(self, run_event: ExecutionStatus):
        return True

    async def get_run_by_run_key(self, runnable_name, run_key):
        return None
