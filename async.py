import asyncio
import time
from functools import wraps, partial
from enum import Enum


class ExecutionStatus(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"


class ExecutionResult:

    def __init__(self, status):
        self.status = status
        self.payload = {}


class Task:

    def __init__(self, fun):
        self.fun = fun

    async def __call__(self, *args, **kwargs):
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(None, partial(self.fun, *args, **kwargs))
        return res


def task(fun):
    return Task(fun)


def manager(fun):
    @wraps(fun)
    async def decorated():
        return await fun()
    return decorated


@task
def task_1(x):
    time.sleep(1)
    return x + 1


@task
def task_2(x):
    time.sleep(1)
    return x * 2


@manager
async def control_flow1():
    a = await task_1(1)
    print(a)
    b = await task_2(a)
    print(b)


@manager
async def control_flow2():
    a = await task_1(2)
    print(a)
    b = await task_2(a)
    print(b)


async def run_managers():
    asyncio.create_task(control_flow1())
    asyncio.create_task(control_flow2())
    print("spawned")


if __name__ == "__main__":
    asyncio.run(run_managers())
