import threading, collections, asyncio


class Singleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                # Another thread could have created the instance
                # before we acquired the lock. So check that the
                # instance is still nonexistent.
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance


class Task:
    def __init__(self, payload, max_concurrency=None):
        self.payload = payload
        self.semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None


class TaskQueue:
    def __init__(self):
        self._deque = collections.deque()

    def queue_task(self, task):
        self._deque.appendleft(task)

    def get_next_task(self):
        top = self._deque.pop()
        locked_tasks = None

        if top.semaphore is None or not top.semaphore.locked():



class Instance(Singleton):

    task_queue = TaskQueue()
    event_loop = None

    @staticmethod
    def set_event_loop():
        Instance.event_loop = asyncio.get_running_loop()

    @staticmethod
    async def queue_task(self):
        pass


async def main():
    instance = Instance()
    instance.set_event_loop()
    t1 = Task({"hello": "world"})
    res = instance.queue_task(t1)



if __name__ == "__main__":

    asyncio.run()