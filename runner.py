import logging
import subprocess
from multiprocessing import Queue

from microflow.runnable import ExecutionStatus

class Runner:

    def __init__(self, flow):
        self._exec_handles = {}
        self._flow = flow

    async def run_async(self, runnable, *args):

        exec_status = self._flow.should_run(runnable, *args)
        self._flow.log_event(exec_status)

        if exec_status.status is ExecutionStatus.QUEUED:
            async with self._flow.wait_in_queue(runnable):
                exec_status = exec_status.get_new_status(status=ExecutionStatus.STARTED)
                self._flow.log_event(exec_status)
                run_handle = self._start_run(runnable, *args)
                self._exec_handles[exec_status.run_id] = run_handle
                result = await run_handle.result()

                self._event_logger.log(exec)

    def _start_run(self, runnable, *args):
        pass



class InProcessRunner(Runner):
    pass

class ProcessPoolRunner(Runner):
    pass

def worker_function(jobs_queue: Queue, results_queue: Queue):
    for task_name, args in iter(jobs_queue.get, "STOP"):
        task = instance.tasks[task_name]
        res = None
        try:
            res = task.fun(*args)
            status = ExecutionStatus.SUCCESS
        except Exception as e:
            logging.error(e)
            status = ExecutionStatus.ERROR
        results_queue.put(ExecutionStatus(status, output=res))


class :

    def __init__(self):
        self.workers = []

    def wait_worker(self, process_id):

        proc = self.workers[process_id]

        def read_output(line):
            try:
                output = line.decode("utf-8")
            except UnicodeDecodeError:
                output = line.decode("latin-1")

        while proc.poll() is None:
            read_output(proc.stdout.readline())

        # Make sure stdout is exhausted
        for output_line in iter(proc.stdout.readline, b""):
            read_output(output_line)

        return proc.poll()

    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
    )


