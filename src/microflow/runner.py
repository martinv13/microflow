import asyncio
import logging
import inspect
import threading
import queue
from abc import ABC, abstractmethod

from multiprocess import Process, Queue, Event

from microflow.utils import EventLoopStore

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from microflow.runnable import RunnableStore

logger = logging.getLogger(__name__)


class BaseRunner(ABC):
    def __init__(self, runnable_store: "RunnableStore" = None):
        self._exec_handles = {}
        self._tasks = None
        self._runnable_store = runnable_store

    def set_tasks(self, tasks: dict):
        self._tasks = tasks

    @abstractmethod
    async def run(self, runnable_name: str, run_id: str, run_args: tuple) -> dict:
        pass

    async def terminate(self, run_id: str):
        pass

    async def start(self):
        if self._tasks is None and self._runnable_store is not None:
            self._tasks = self._runnable_store.get_functions_as_dict()
        if not isinstance(self._tasks, dict):
            raise RuntimeError("Runner needs tasks dict to start")

    async def shutdown(self):
        pass


class ThreadingRunner(BaseRunner):
    async def run(self, runnable_name: str, run_id: str, run_args: tuple) -> dict:
        fun = self._tasks[runnable_name]
        try:
            if inspect.iscoroutinefunction(fun):
                res = await fun(*run_args)
            else:
                res = await asyncio.to_thread(fun, *run_args)
            return {
                "type": "result",
                "run_id": run_id,
                "result": res,
            }
        except Exception as e:
            return {
                "type": "error",
                "run_id": run_id,
                "result": e,
            }


def process_worker(result_queue: Queue, fun, run_id, run_args):
    logger.info(f"Worker ready to process job {run_id}")
    try:
        if inspect.iscoroutinefunction(fun):
            res = asyncio.run(fun(*run_args))
        else:
            res = fun(*run_args)
        result_queue.put(
            {
                "type": "result",
                "run_id": run_id,
                "result": res,
            }
        )
    except Exception as e:
        result_queue.put(
            {
                "type": "error",
                "run_id": run_id,
                "result": e,
            }
        )


class MultiprocessRunner(BaseRunner):
    def run_wrapper(self, runnable_name: str, run_id: str, run_args: tuple):
        result_queue = Queue()
        process = Process(
            target=process_worker,
            args=(result_queue, self._tasks[runnable_name], run_id, run_args),
        )
        self._exec_handles[run_id] = process
        process.start()
        process.join()
        res = result_queue.get(block=False)
        if res:
            return res
        else:
            logger.info(
                f"Process running job {run_id} exited with code {process.exitcode} and did not return a value"
            )
            return {
                "type": "error",
                "run_id": run_id,
                "result": f"Process did not return a value and exited with code {process.exitcode}",
            }

    async def start(self):
        if self._tasks is None and self._runnable_store is not None:
            self._tasks = self._runnable_store.get_functions_as_dict(tasks_only=True)

    async def run(self, runnable_name: str, run_id: str, run_args: tuple):
        return await asyncio.to_thread(
            self.run_wrapper, runnable_name, run_id, run_args
        )


def pool_process_worker(process_key, jobs_queue, results_queue, fun_dict):
    """Worker function to run in a subprocess

    :param process_key: A numeric process id
    :param jobs_queue: A queue to pass jobs to the process
    :param results_queue: A queue to receive results from the process
    :param fun_dict: A dict of functions to run
    """
    results_queue.put({"type": "ack_process_start"})
    logger.info(f"Worker {process_key} is waiting for jobs")
    for task_name, run_id, run_args in iter(jobs_queue.get, "STOP"):
        task_fun = fun_dict.get(task_name)
        try:
            if inspect.iscoroutinefunction(task_fun):
                res = asyncio.run(task_fun(*run_args))
            else:
                res = task_fun(*run_args)
            results_queue.put(
                {
                    "type": "result",
                    "run_id": run_id,
                    "result": res,
                }
            )
        except Exception as e:
            results_queue.put(
                {
                    "type": "error",
                    "run_id": run_id,
                    "result": e,
                }
            )


class ProcessPoolRunner(BaseRunner):
    """Runs a pool of process to run jobs"""

    def __init__(
        self,
        loop_store: EventLoopStore,
        nb_workers: int,
        runnable_store: "RunnableStore" = None,
    ):
        super().__init__()
        self._loop_store = loop_store
        self._runnable_store = runnable_store
        self._background_tasks = set()

        # Events to manage the status of the runner
        self._started = threading.Event()
        self._shutdown = threading.Event()
        self._stopped = asyncio.Semaphore(0)
        self._error = threading.Event()

        # Variables to manage the subprocesses
        self._nb_workers = nb_workers
        self._max_workers_semaphore = asyncio.Semaphore(nb_workers - 1)
        self._workers_processes = {}
        self._next_process_key = 0

        # Main queues used to communicate with the subprocesses
        self._jobs_queue = None
        self._results_queue = None

        # Mapping for running jobs
        self._runs_process_map = {}
        self._runs_process_map_lock = threading.Lock()
        self._runs_results = {}
        self._runs_results_lock = threading.Lock()

    def _worker_wrapper(self, process_key, jobs_queue, results_queue):
        """A function that runs in a thread, create a subprocess and communicate with it using its own queues,
        in order not to corrupt the main queues if the subprocess is terminated from the outside.

        """
        process_jobs_queue = Queue()
        process_results_queue = Queue()

        try:
            process = Process(
                target=pool_process_worker,
                args=[
                    process_key,
                    process_jobs_queue,
                    process_results_queue,
                    self._tasks,
                ],
            )
            process_stopped_event = threading.Event()
            self._workers_processes[process_key] = (
                process,
                process_results_queue,
                process_stopped_event,
            )
            process.start()
            # wait for initial acknowledgment that the process has started and is waiting for jobs
            # if not received within 5s, we consider that the process failed to start
            results_queue.put(process_results_queue.get(timeout=5))
            while not self._shutdown.is_set() and not process_stopped_event.is_set():
                next_job = jobs_queue.get()
                if next_job == "STOP":
                    process_jobs_queue.put("STOP")
                    break
                task_name, run_id, run_args = next_job
                with self._runs_process_map_lock:
                    self._runs_process_map[run_id] = process_key
                process_jobs_queue.put((task_name, run_id, run_args))
                res = process_results_queue.get()
                results_queue.put(res)
            process_jobs_queue.put("STOP")
            process.join()
            if process.exitcode != 0:
                self._error.set()
                self._loop_store.shutdown_event.set()
        except Exception as e:
            logger.info(f"Error while starting worker: {e}")
            self._shutdown.set()
            self._error.set()

    async def _run_pool(self):
        """A function that runs as an asyncio task and keep starting worker threads."""
        while not self._shutdown.is_set():
            process_key = self._next_process_key
            logger.info(f"Spawing worker {process_key}")
            self._next_process_key += 1
            run_worker = asyncio.create_task(
                asyncio.to_thread(
                    self._worker_wrapper,
                    process_key,
                    self._jobs_queue,
                    self._results_queue,
                )
            )
            self._background_tasks.add(run_worker)

            def run_worker_callback(task):
                self._background_tasks.discard(task)
                self._max_workers_semaphore.release()
                self._stopped.release()

            run_worker.add_done_callback(run_worker_callback)

            await self._max_workers_semaphore.acquire()

    def _receive_results(self) -> None:
        """A function that runs in a thread, collect run results from the main result queue and set values
        to result futures in order to unlock awaiting calls to the `run` method.

        """
        loop = self._loop_store.get_loop()

        def set_result(res):
            fut = self._runs_results[res["run_id"]]
            if not fut.cancelled():
                fut.set_result(res)

        started_workers = 0

        for res in iter(self._results_queue.get, "STOP"):
            if res["type"] == "ack_process_start":
                started_workers += 1
                if started_workers == self._nb_workers:
                    self._started.set()
            else:
                loop.call_soon_threadsafe(set_result, res)

    async def start(self) -> None:
        """Starts the worker processes and wait until they are ready to process jobs."""
        logger.info("Starting up process pool runner")

        if self._tasks is None and self._runnable_store is not None:
            self._tasks = self._runnable_store.get_functions_as_dict(tasks_only=True)
        if not isinstance(self._tasks, dict):
            raise RuntimeError(
                "No tasks to run. Please set a RunnableStore or call set_tasks before starting."
            )

        self._jobs_queue = queue.Queue()
        self._results_queue = queue.Queue()

        self._loop_store.get_loop()

        # run a thread to process results sent to the results queue
        receive_results_thread = asyncio.create_task(
            asyncio.to_thread(self._receive_results)
        )
        self._background_tasks.add(receive_results_thread)

        def background_task_callback(task):
            self._background_tasks.discard(task)
            self._stopped.release()

        receive_results_thread.add_done_callback(background_task_callback)

        # run a background task to start (and restart) process workers
        run_pool = asyncio.create_task(self._run_pool())
        self._background_tasks.add(run_pool)
        run_pool.add_done_callback(background_task_callback)

        # wait for successful start up or error, for 10s
        for _ in range(50):
            await asyncio.sleep(0.2)
            if self._started.is_set():
                break
            if self._error.is_set():
                raise RuntimeError("Runner failed to start due to an error")

        if not self._started.is_set():
            raise RuntimeError("Runner failed to start after 10s")

        logger.info("Process pool runner started")

    async def shutdown(self) -> None:
        """Shut down all running processes and tasks. It waits for actual termination."""
        if not self._jobs_queue:
            return
        logger.info("Shutting down process pool runner")
        self._shutdown.set()
        for i in range(self._nb_workers):
            self._jobs_queue.put("STOP")
        self._results_queue.put("STOP")
        # wait until background tasks are done (callbacks release the _stopped semaphore)
        for _ in range(3):
            await self._stopped.acquire()
        await asyncio.sleep(0.2)

    async def run(self, runnable_name: str, run_id: str, run_args: tuple) -> dict:
        """Run a job in the runner and await the result.

        :param runnable_name: The function key in the dict of functions to run
        :param run_id: An id for this run
        :param run_args: Arguments passed to the function call
        :return: The result of the run, wrapped in a dict which indicates the status
        """
        if self._shutdown.is_set():
            raise RuntimeError("Process pool failed to start or was already shut down")
        loop = self._loop_store.get_loop()
        self._runs_results[run_id] = loop.create_future()
        self._jobs_queue.put((runnable_name, run_id, run_args))
        res = await self._runs_results[run_id]
        del self._runs_results[run_id]
        del self._runs_process_map[run_id]
        return res

    async def terminate(self, run_id: str) -> None:
        """Terminate a run if it is currently running. Kills the process and return a terminated result.
        Noop if the run_id does not correspond to a running job.

        :param run_id: The run_id to terminate
        """
        logger.info(f"Termination requested for run '{run_id}'")
        if run_id in self._runs_process_map:
            process_key = self._runs_process_map[run_id]
            process, result_queue, shutdown_event = self._workers_processes[process_key]
            shutdown_event.set()
            result_queue.put(
                {
                    "type": "terminated",
                    "run_id": run_id,
                }
            )
            result_queue.put("STOP")
            process.kill()
        else:
            logger.info(
                f"Tried to terminate '{run_id}' which is not currently running, noop"
            )
