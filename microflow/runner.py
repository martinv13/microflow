import logging
import subprocess
from multiprocessing import Queue


class Runner:
    def __init__(self):
        self._exec_handles = {}

    async def run(self, runnable, run_id, run_args):
        return {}


class ThreadingRunner(Runner):
    pass


class MultiprocessingRunner(Runner):
    pass
