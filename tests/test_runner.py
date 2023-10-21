import asyncio
import json
import time

import pytest

from microflow.utils import EventLoopStore
from microflow.runner import ThreadingRunner, MultiprocessRunner, ProcessPoolRunner


@pytest.mark.parametrize(
    "runner_config",
    [
        (ThreadingRunner, {}),
        (MultiprocessRunner, {}),
        (ProcessPoolRunner, {"nb_workers": 2, "loop_store": EventLoopStore()}),
    ],
)
@pytest.mark.asyncio
async def test_runner(runner_config):
    def fun1(a: bool):
        time.sleep(1)
        if a:
            return {"hello": "world"}

    def fun2():
        time.sleep(1)
        raise RuntimeError("something happened")

    runner_cls, runner_kwargs = runner_config
    runner = runner_cls(**runner_kwargs)
    runner.set_tasks(
        {
            "a": fun1,
            "b": fun2,
        }
    )
    await runner.start()

    res = await asyncio.gather(
        asyncio.create_task(runner.run("a", "1", (True,))),
        asyncio.create_task(runner.run("a", "2", (False,))),
        asyncio.create_task(runner.run("b", "3", ())),
    )

    await runner.shutdown()

    assert str(res) == (
        "[{'type': 'result', 'run_id': '1', 'result': {'hello': 'world'}}, "
        "{'type': 'result', 'run_id': '2', 'result': None}, "
        "{'type': 'error', 'run_id': '3', 'result': RuntimeError('something happened')}]"
    )


@pytest.mark.asyncio
async def test_termination():
    def fun1(value: int):
        time.sleep(2)
        return value

    async def wait_and_terminate():
        await asyncio.sleep(2.5)
        await runner.terminate("run_2")

    runner = ProcessPoolRunner(nb_workers=2, loop_store=EventLoopStore())
    runner.set_tasks({"a": fun1})

    await runner.start()

    res = await asyncio.gather(
        *(
            [asyncio.create_task(runner.run("a", f"run_{i}", (i,))) for i in range(5)]
            + [asyncio.create_task(wait_and_terminate())]
        )
    )

    await runner.shutdown()

    assert json.dumps(res, sort_keys=True) == json.dumps([
        {'type': 'result', 'run_id': 'run_0', 'result': 0},
        {'type': 'result', 'run_id': 'run_1', 'result': 1},
        {'type': 'terminated', 'run_id': 'run_2'},
        {'type': 'result', 'run_id': 'run_3', 'result': 3},
        {'type': 'result', 'run_id': 'run_4', 'result': 4},
        None
    ], sort_keys=True)


if __name__ == "__main__":
    test_runner()
    test_termination()