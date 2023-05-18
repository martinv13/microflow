import asyncio
import time

from microflow.run_queue import RunQueue, ConcurrencyGroup

start_time = time.perf_counter()


def print_with_time(msg):
    print(f"{round((time.perf_counter() - start_time))}s : {msg}")


async def main():

    q = RunQueue()

    cg1 = ConcurrencyGroup(max_concurrency=1, name="cg1")
    cg2 = ConcurrencyGroup(max_concurrency=2, name="cg2")

    async def runner(i):

        print_with_time(f"waiting for task {i} to start")

        task_type = i % 2
        cg = cg1 if task_type == 0 else cg2

        async with q.wait_in_queue(concurrency_group=[cg]):
            print_with_time(f"task {i} started - counter: {cg.name} : {cg.counter}")
            await asyncio.sleep(4 if task_type == 0 else 2)
            print_with_time(f"task {i} done")

    tasks = [i+1 for i in range(10)]

    await asyncio.gather(*(runner(i) for i in tasks))

if __name__ == "__main__":
    asyncio.run(main())