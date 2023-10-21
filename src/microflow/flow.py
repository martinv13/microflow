import logging
from typing import Callable, Union
import asyncio
import functools
import uuid
from multiprocessing import cpu_count

from microflow.utils import EventLoopStore
from microflow.runnable import RunStrategy, Task, Manager, RunnableStore, Runnable
from microflow.run_queue import RunQueue, ConcurrencyGroup
from microflow.event_store import EventStore, EventStoreMemoryAdapter
from microflow.runner import BaseRunner, ThreadingRunner, ProcessPoolRunner
from microflow.api import FlowServer

logger = logging.getLogger(__name__)


class Schedule:
    def __init__(self, expr, name=None):
        self.schedule_expr = expr
        self.name = name


class Flow:
    """The main class to represent a workflow."""

    def __init__(
        self,
        loop_store: EventLoopStore,
        runnable_store: RunnableStore,
        queue: RunQueue,
        event_store: EventStore,
        tasks_runner: BaseRunner,
        managers_runner: BaseRunner,
        global_task_concurrency: int,
        flow_name: str = None,
    ):
        """A Flow is created with service classes providing different features. Following a dependency injection
        principle, instances of these classes are provided to the constructor.

        :param loop_store: A `LoopStore` instance, which holds a reference to the running loop
        :param runnable_store: A `RunnableStore` instance, which holds tasks and managers
        :param queue: An `RunQueue` instance that will manage concurrency
        :param event_store: An instance of `EventStoreAdapter` to store run events
        :param tasks_runner: A runner for tasks
        :param managers_runner: A runner for managers
        :param global_task_concurrency: The global max concurrency for tasks
        :param flow_name: A name for this `Flow`
        """
        self.flow_name = flow_name
        self._init = False
        self._stopped = False
        self._background_tasks = set()

        self._global_tasks_concurrency = ConcurrencyGroup(
            global_task_concurrency, name="GLOBAL_TASKS_CONCURRENCY"
        )
        self.loop_store = loop_store
        self.runnables = runnable_store
        self.queue = queue
        self.event_store = event_store
        self._tasks_runner = tasks_runner
        self._managers_runner = managers_runner
        self.flow_server = None

    def task(
        self,
        decorated_func: Callable = None,
        *,
        name: str = None,
        inputs: list[str] = None,
        max_concurrency: Union[
            ConcurrencyGroup, list[ConcurrencyGroup], int, None
        ] = None,
        schedule: Union[Schedule, str, None] = None,
        run_strategy: str = RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        """Decorate a function to create and register a new task within the `Flow`

        :param decorated_func: The function to be added as a task
        :param name: A name for the task (defaults to the function's name)
        :param inputs: An optional list of inputs for the task (names of other tasks or managers)
        :param max_concurrency:
        :param schedule:
        :param run_strategy:
        :return:
        """
        if decorated_func is None:
            return functools.partial(
                self.task,
                name=name,
                inputs=inputs,
                max_concurrency=max_concurrency,
                schedule=schedule,
                run_strategy=run_strategy,
            )
        runnable = Task(
            flow=self,
            runner=self._tasks_runner,
            fun=decorated_func,
            name=name,
            inputs=inputs,
            max_concurrency=max_concurrency,
            schedule=schedule,
            run_strategy=run_strategy,
        )
        runnable.concurrency_groups.append(self._global_tasks_concurrency)
        self.runnables.register(runnable)
        return runnable

    def manager(
        self,
        decorated_func=None,
        *,
        name=None,
        inputs=None,
        schedule=None,
        run_strategy=RunStrategy.ANY_SUCCESS_NO_ERROR,
    ):
        if decorated_func is None:
            return functools.partial(
                self.manager,
                name=name,
                inputs=inputs,
                schedule=schedule,
                run_strategy=run_strategy,
            )
        runnable = Manager(
            flow=self,
            runner=self._managers_runner,
            fun=decorated_func,
            name=name,
            inputs=inputs,
            schedule=schedule,
            run_strategy=run_strategy,
        )
        self.runnables.register(runnable)
        return runnable

    async def init(self):
        if self._init:
            return

        # TODO: check DAGs
        # raise RuntimeError("config error")

        _loop = self.loop_store.get_loop(start=True)

        def wait_shutdown():
            self.loop_store.shutdown_event.wait()
            res = asyncio.run_coroutine_threadsafe(self.shutdown(), _loop)
            return res.result()

        wait_shutdown_task = asyncio.create_task(asyncio.to_thread(wait_shutdown))
        self._background_tasks.add(wait_shutdown_task)
        wait_shutdown_task.add_done_callback(self._background_tasks.discard)

        try:
            if self.runnables.has_tasks():
                await asyncio.gather(
                    self._tasks_runner.start(),
                    self._managers_runner.start(),
                )
            else:
                await self._managers_runner.start()
        except Exception as e:
            logger.error(e)
            self.loop_store.shutdown_event.set()

        self._init = True

    async def shutdown(self):
        if self._stopped:
            return
        self._stopped = True
        self.loop_store.shutdown_event.set()
        if self.runnables.has_tasks():
            await asyncio.gather(
                self._tasks_runner.shutdown(),
                self._managers_runner.shutdown(),
            )
        else:
            await self._managers_runner.shutdown()
        self.loop_store.stop_loop()

    def run_coro(self, coro):
        loop = self.loop_store.get_loop()
        # if we already have a running loop, simply run in this loop
        if loop is not None:
            fut = asyncio.run_coroutine_threadsafe(coro, loop)
            return fut.result()

        # if not, it means that a manager or task was called directly, so we start the Flow,
        # run the job and shut it down
        async def init_run_shutdown():
            coro_task = None
            try:
                await self.init()
                if self._init:
                    coro_task = asyncio.create_task(coro)
                    res = await coro_task
                    await self.shutdown()
                    return res
            except Exception as e:
                logger.error(e)
                if coro_task is not None:
                    coro_task.cancel()
                raise e
            else:
                raise RuntimeError("Flow failed to initialize.")

        return asyncio.run(init_run_shutdown())

    async def start_run(self, runnable: Union[str, Runnable], *args, sync=False):
        if isinstance(runnable, Runnable):
            target = runnable
        else:
            target = self.runnables.get(runnable)
        loop = self.loop_store.get_loop()
        if sync:
            res = await target.run_async(*args)
            run_id = res.run_id
        else:
            run_id = str(uuid.uuid4())
            task = loop.create_task(target.run_async(*args, run_id=run_id))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

        return run_id

    def serve(self, host="localhost", port=3000):
        if self.flow_server is None:
            self.flow_server = FlowServer(flow=self)
        self.flow_server.serve(host=host, port=port)


def create_flow(flow_name=None, global_tasks_concurrency=cpu_count()):
    loop_store = EventLoopStore()
    runnable_store = RunnableStore()
    return Flow(
        loop_store=loop_store,
        runnable_store=runnable_store,
        queue=RunQueue(),
        event_store=EventStore(EventStoreMemoryAdapter()),
        tasks_runner=ProcessPoolRunner(
            loop_store=loop_store,
            nb_workers=global_tasks_concurrency,
            runnable_store=runnable_store,
        ),
        managers_runner=ThreadingRunner(runnable_store=runnable_store),
        global_task_concurrency=global_tasks_concurrency,
        flow_name=flow_name,
    )
