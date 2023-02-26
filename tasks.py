import concurrent.futures
import threading
import asyncio
import functools


class Singleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance


class Instance(Singleton):

    _managers = {}
    _tasks = {}
    _loop = None
    _executor_pool = concurrent.futures.ProcessPoolExecutor()

    @staticmethod
    def register_manager(manager):
        if manager.name in Instance._managers:
            raise RuntimeError(f"manager {manager.name} is already defined")
        Instance._managers[manager.name] = manager

    @staticmethod
    def register_task(task):
        if task.name in Instance._tasks:
            raise RuntimeError(f"task {task.name} is already defined")
        Instance._tasks[task.name] = task

    @staticmethod
    def register_loop(loop):
        Instance._loop = loop

    @staticmethod
    def get_loop():
        if Instance._loop is None:
            raise RuntimeError("loop was not set")
        return Instance._loop

    @staticmethod
    async def run_manager(manager_name, *args, **kwargs):
        loop = asyncio.get_running_loop()
        Instance.register_loop(loop)
        if manager_name not in Instance._managers:
            raise RuntimeError(f"manager '{manager_name}' is not defined")
        await Instance._managers[manager_name](*args, **kwargs)

    @staticmethod
    async def run_task(task_obj, *args):
        return await Instance._loop.run_in_executor(
            Instance._executor_pool,
            task_obj.run_in_worker,
            *args,
        )


class ConcurrencyGroup:
    def __init__(self, max_concurrency=1, name=None):
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.name = name


def concurrency_group(max_concurrency=1):
    return ConcurrencyGroup(max_concurrency)


class Task:
    def __init__(self, fun, name=None, max_concurrency=None):
        self.fun = fun
        self.name = fun.__name__ if name is None else name
        self.instance = Instance()
        self.instance.register_task(self)
        self.concurrency_group = (
            None
            if max_concurrency is None
            else max_concurrency
            if isinstance(max_concurrency, ConcurrencyGroup)
            else ConcurrencyGroup(max_concurrency)
        )
        self.__module__ = self.fun.__module__

    def run_in_worker(self, *args):
        return self.fun(*args)

    async def run_task(self, *args):
        if self.concurrency_group is None:
            return await self.instance.run_task(self, *args)
        async with self.concurrency_group.semaphore:
            return await self.instance.run_task(self, *args)

    def __call__(self, *args, **kwargs):
        future = asyncio.run_coroutine_threadsafe(
            self.run_task(*args),
            self.instance.get_loop(),
        )
        return future.result()

    def __reduce__(self):
        return self.fun.__qualname__


def task(func=None, *, name=None, max_concurrency=None):
    if func is None:
        return functools.partial(task, name=name, max_concurrency=max_concurrency)
    return Task(func, name=name, max_concurrency=max_concurrency)


class Manager:
    def __init__(self, func, name=None):
        self.func = func
        self.name = func.__name__ if name is None else name
        self.instance = Instance()
        self.instance.register_manager(self)

    async def __call__(self, *args, **kwargs):
        await asyncio.to_thread(self.func, *args, **kwargs)


def manager(func=None, *, name=None):
    if func is None:
        return functools.partial(manager, name=name)
    return Manager(func, name=name)


class RunThread(threading.Thread):
    def __init__(self, target, args):
        super().__init__()
        self.target_fun = target
        self.args = args
        self.return_value = None

    def run(self):
        self.return_value = self.target_fun(self.args)


def run_parallel(*args):
    threads = []
    for task_call in args:
        if not callable(task_call[0]):
            raise ValueError("first argument of run_parallel must be callable")
        threads.append(
            RunThread(
                target=task_call[0],
                args=task_call[1 : (len(task_call) + 1)]
                if len(task_call) > 2
                else task_call[1]
                if len(task_call) == 2
                else None,
            )
        )
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return (t.return_value for t in threads)
