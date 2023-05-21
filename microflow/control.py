import asyncio


async def run_with_dependencies_async(steps):
    """Run a set of Managers or Tasks in DAG order, running their dependencies first

    :param steps: a list of Managers or Tasks
    :return: the return values of the Managers or Tasks
    """
    input_done_events = {}
    input_values = {}

    async def run_step(step):
        if step.name in input_done_events:
            await input_done_events[step.name].wait()
        else:
            input_done_events[step.name] = asyncio.Event()
            inputs = (
                await asyncio.gather(
                    *(run_step(step_input) for step_input in step.inputs)
                )
                if len(step.inputs) > 0
                else []
            )
            input_values[step.name] = await step.run_async(*inputs)
            input_done_events[step.name].set()

        return input_values[step.name]

    return await asyncio.gather(*(run_step(step) for step in steps))


def run_parallel(*args, loop=None):
    """Run Tasks or managers in parallel from a sync function

    :param args: either tasks or managers or
    :param loop:
    :return:
    """
    if loop is None:
        loop = asyncio.get_running_loop()
    coros = []
    for task_call in args:
        if not callable(task_call[0]):
            raise ValueError("first argument of run_parallel must be callable")
        if len(task_call) > 2:
            coros.append(task_call[0](*task_call[1 : (len(task_call) + 1)]))
        elif len(task_call) == 2:
            coros.append(task_call[0](task_call[1]))
        else:
            coros.append(task_call[0]())
    fut = asyncio.run_coroutine_threadsafe(
        asyncio.gather(*coros),
        loop,
    )
    return fut.result()


def map_parallel(func, arg, loop=None):
    if loop is None:
        loop = asyncio.get_running_loop()
    coros = []
    for task_call in args:
        if not callable(task_call[0]):
            raise ValueError("first argument of run_parallel must be callable")
        if len(task_call) > 2:
            coros.append(task_call[0](*task_call[1 : (len(task_call) + 1)]))
        elif len(task_call) == 2:
            coros.append(task_call[0](task_call[1]))
        else:
            coros.append(task_call[0]())
    fut = asyncio.run_coroutine_threadsafe(
        asyncio.gather(*coros),
        loop,
    )
    return fut.result()
