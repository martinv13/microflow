import asyncio
from collections import deque


class ConcurrencyGroup:
    def __init__(self, max_concurrency=1, name=None):
        self.max_concurrency = max_concurrency
        self.counter = 0
        self.name = name

    def acquire(self, dry=False):
        acquirable = self.counter < self.max_concurrency
        if acquirable and not dry:
            self.counter += 1
        return acquirable

    def release(self):
        if self.counter > 0:
            self.counter -= 1
            return True
        return False


def acquire_all(concurrency_groups):
    """Try to acquire simultaneously multiple ConcurrencyGroup objects

    :param concurrency_groups: a list of ConcurrencyGroup objects to acquire
    :return: True if lock was acquired, False if not
    """
    if all(map(lambda g: g.acquire(dry=True), concurrency_groups)):
        for g in concurrency_groups:
            g.acquire()
        return True
    return False


class WaitingContextManager:
    def __init__(self, run_queue, start_future, concurrency_groups):
        self.run_queue = run_queue
        self.start_future = start_future
        self.concurrency_groups = concurrency_groups

    async def __aenter__(self):
        await self.start_future

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for g in self.concurrency_groups:
            g.release()
        self.run_queue.wake_up_first()


class RunQueue:
    def __init__(self, max_concurrency=4):
        self.waiters = None
        self.global_concurrency = ConcurrencyGroup(max_concurrency=max_concurrency)

    def wait_in_queue(self, concurrency_group=None):
        """Create an async context manager to wait in the execution queue

        :param concurrency_group: a ConcurrencyGroup object or a list of ConcurrencyGroup objects to respect
        :return: an async context manager which makes the function wait before it can be executed, and release
        the locks after execution
        """
        if concurrency_group is None:
            concurrency_group = []
        elif isinstance(concurrency_group, ConcurrencyGroup):
            concurrency_group = [concurrency_group]
        elif not isinstance(concurrency_group, list):
            raise ValueError()

        concurrency_groups = [self.global_concurrency] + concurrency_group

        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        if acquire_all(concurrency_groups):
            fut.set_result(True)
        else:
            if self.waiters is None:
                self.waiters = deque()
            self.waiters.appendleft((concurrency_groups, fut))

        return WaitingContextManager(
            run_queue=self, start_future=fut, concurrency_groups=concurrency_groups
        )

    def wake_up_first(self):
        """Release the next waiter in line to start its execution, if any"""
        if len(self.waiters) > 0:

            top_concurrency_groups, top_fut = self.waiters.pop()
            if acquire_all(top_concurrency_groups):
                top_fut.set_result(True)
            else:
                stack = deque()
                stack.append((top_concurrency_groups, top_fut))
                exec_found = False
                while len(self.waiters) > 0:
                    top_concurrency_groups, top_fut = self.waiters.pop()
                    exec_found = acquire_all(top_concurrency_groups)
                    if exec_found:
                        break
                    stack.append((top_concurrency_groups, top_fut))
                while len(stack) > 0:
                    self.waiters.append(stack.pop())
                if exec_found:
                    top_fut.set_result(True)
