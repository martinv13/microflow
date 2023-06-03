import asyncio
import logging
import inspect
import dill
from multiprocess import Process, Queue

from microflow.exec_result import ExecutionResult
from microflow.utils import EventLoopStore

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from microflow.runnable import RunnableStore

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger()


class Runner:
    def __init__(self):
        self._exec_handles = {}
        self._tasks = None

    def set_tasks(self, tasks):
        self._tasks = tasks

    async def run(self, runnable, run_id, run_args):
        return {}

    async def terminate(self, run_id):
        pass

    async def start(self):
        pass

    async def shutdown(self):
        pass


class ThreadingRunner(Runner):
    async def run(self, runnable, run_id, run_args):
        if runnable.fun is not None:
            return await asyncio.to_thread(runnable.fun, *run_args)

        if runnable.async_fun is not None:
            return await runnable.async_fun(*run_args)


class MultiprocessRunner(Runner):
    @staticmethod
    def worker(self, result_queue: Queue, runnable, run_id, run_args):
        try:
            if runnable.fun is not None:
                res = runnable.fun(*run_args)
            else:
                res = asyncio.run(runnable.async_fun(*run_args))
        except Exception as e:
            res = ExecutionResult(
                status=ExecutionResult.ERROR,
                reason=f"execution failed: {e}",
            )
        result_queue.put(res)

    def run_wrapper(self, runnable, run_id, run_args):
        result_queue = Queue()
        process = Process(
            target=self.worker,
            args=(result_queue, runnable, run_id, run_args),
        )
        self._exec_handles[run_id] = process
        process.start()
        process.join()
        return result_queue.get(block=False)

    async def run(self, runnable, run_id, run_args):
        return await asyncio.to_thread(self.run_wrapper, runnable, run_id, run_args)


def process_pool_worker(process_key, jobs_queue, results_queue, fun_dict):
    logger.info(f"hello from worker {process_key}")
    for task_name, run_id, run_args in iter(jobs_queue.get, "STOP"):
        results_queue.put({"type": "ack", "run_id": run_id, "process_key": process_key})
        task_fun = fun_dict.get(task_name)
        try:
            if inspect.iscoroutinefunction(task_fun):
                res = asyncio.run(task_fun(*run_args))
            else:
                res = task_fun(*run_args)
            if res is None:
                res = ExecutionResult(status=ExecutionResult.SKIPPED)
            elif not isinstance(res, ExecutionResult) and isinstance(res, dict):
                res = ExecutionResult(status=ExecutionResult.SUCCESS, value=res)
            elif not isinstance(res, dict):
                res = ExecutionResult(
                    status=ExecutionResult.ERROR, reason="output must be a dict"
                )
        except Exception as e:
            res = ExecutionResult(
                status=ExecutionResult.ERROR, reason=f"execution failed: {e}"
            )
        results_queue.put(
            {
                "type": "res",
                "process_key": process_key,
                "run_id": run_id,
                "result": res,
            }
        )


class ProcessPoolRunner(Runner):
    def __init__(
        self,
        loop_store: EventLoopStore,
        nb_workers: int,
        runnable_store: "RunnableStore",
    ):
        super().__init__()
        self._loop_store = loop_store
        self._runnable_store = runnable_store
        self._background_tasks = set()

        self._started = False
        self._stopped = False

        self._nb_workers = nb_workers
        self._max_workers_semaphore = asyncio.Semaphore(nb_workers)
        self._workers_processes = {}
        self._next_process_key = 0

        self._jobs_queue = None
        self._results_queue = None

        self._runs_process_keys = {}
        self._runs_results = {}

    def _worker_wrapper(self, process_key, jobs_queue, results_queue):
        process = Process(
            target=process_pool_worker,
            args=[
                process_key,
                jobs_queue,
                results_queue,
                self._runnable_store.get_tasks_functions_as_dict(),
            ],
        )
        self._workers_processes[process_key] = process
        process.start()
        process.join()

    async def _run_pool(self):
        while not self._stopped:
            await self._max_workers_semaphore.acquire()
            if self._stopped:
                break
            process_key = self._next_process_key
            logger.info(f"spawing worker {process_key}")
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
                logger.info(f"worker {process_key} terminated")
                self._background_tasks.discard(task)
                self._max_workers_semaphore.release()

            run_worker.add_done_callback(run_worker_callback)

    def _receive_results(self):
        loop = self._loop_store.get_loop()
        for res in iter(self._results_queue.get, "STOP"):
            if res["type"] == "ack":
                logger.info(
                    f"worker {res['process_key']} acknowledged run {res['run_id']}"
                )
                self._runs_process_keys[res["run_id"]] = res["process_key"]
            elif res["type"] == "res":
                logger.info(f"worker {res['process_key']} returned result")
                loop.call_soon_threadsafe(
                    self._runs_results[res["run_id"]].set_result, res["result"]
                )

    async def start(self):
        logger.info("starting up process pool runner")
        self._jobs_queue = Queue()
        self._results_queue = Queue()

        # run a thread to process results sent to the results queue
        receive_results_thread = asyncio.create_task(
            asyncio.to_thread(self._receive_results)
        )
        self._background_tasks.add(receive_results_thread)
        receive_results_thread.add_done_callback(self._background_tasks.discard)

        # run a background task to start (and restart) process workers
        run_pool = asyncio.create_task(self._run_pool())
        self._background_tasks.add(run_pool)
        run_pool.add_done_callback(self._background_tasks.discard)

        self._started = True

    async def shutdown(self):
        if not self._started:
            return
        logger.info("shutting down process pool runner")
        self._stopped = True
        for i in range(self._nb_workers * 2):
            self._jobs_queue.put("STOP")
        self._results_queue.put("STOP")

    async def run(self, runnable, run_id, run_args):
        if self._stopped:
            raise RuntimeError("pool already stopped")
        loop = self._loop_store.get_loop()
        self._runs_results[run_id] = loop.create_future()
        self._jobs_queue.put((runnable.name, run_id, run_args))
        res = await self._runs_results[run_id]
        del self._runs_results[run_id]
        del self._runs_process_keys[run_id]
        return res

    async def terminate(self, run_id):
        if run_id in self._runs_process_keys:
            self._workers_processes[self._runs_process_keys[run_id]].kill()
