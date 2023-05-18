import time
from microflow import Flow, Schedule

flow = Flow("test flow")


@flow.task(max_concurrency=1)
def task_1(a):
    time.sleep(3)
    print("hello from task 1\n")
    return a + 1


@flow.task
def task_2(b):
    time.sleep(3)
    print("hello from task 2\n")
    return b * 2


@flow.manager
def control_flow1():
    a, b, c = flow.run_parallel(
        (task_1, 1),
        (task_1, 2),
        (task_2, 3),
    )
    print(f"{a} : {b} : {c}")
    print("end of control flow 1\n")


@flow.manager(schedule="* * * 0 *")
def control_flow2():
    a = task_2(2)
    print(a)
    res = task_2(a)
    print(res)
    print("end of control flow 2\n")


if __name__ == "__main__":

    # start = time.time()
    #
    # async def run_managers():
    #     await asyncio.gather(
    #         Instance().run_manager("control_flow1"),
    #         Instance().run_manager("control_flow2"),
    #     )
    #
    # asyncio.run(run_managers())
    #
    # print(f"elapsed time: {time.time() - start}")

    flow.run_server()