import pytest
import asyncio
from microflow import create_flow, ExecutionStatus


@pytest.fixture
def test_flow():
    return create_flow("test_flow", global_tasks_concurrency=2)


@pytest.mark.parametrize("runnable_type", ["MANAGER", "TASK"])
def test_ok_runnable(test_flow, runnable_type):
    @(test_flow.manager if runnable_type == "MANAGER" else test_flow.task)
    def simple_runnable():
        return {"value": "hello world"}

    res = simple_runnable()
    assert res.status == ExecutionStatus.SUCCESS
    assert res["value"] == "hello world"


@pytest.mark.parametrize("runnable_type", ["MANAGER", "TASK"])
def test_skipped_runnable(test_flow, runnable_type):
    @(test_flow.manager if runnable_type == "MANAGER" else test_flow.task)
    def simple_runnable():
        pass

    res = simple_runnable()
    assert res.status == ExecutionStatus.SKIPPED


@pytest.mark.parametrize("runnable_type", ["MANAGER", "TASK"])
def test_error_runnable(test_flow, runnable_type):
    @(test_flow.manager if runnable_type == "MANAGER" else test_flow.task)
    def simple_runnable():
        raise RuntimeError("something went wrong")

    res = simple_runnable()
    assert res.status == ExecutionStatus.ERROR
    assert str(res.output) == str(RuntimeError("something went wrong"))


@pytest.mark.parametrize("runnable_type", ["MANAGER", "TASK"])
def test_ok_runnable_async(test_flow, runnable_type):
    @(test_flow.manager if runnable_type == "MANAGER" else test_flow.task)
    async def simple_runnable():
        await asyncio.sleep(0.1)
        return {"value": "hello world"}

    res = simple_runnable()
    assert res.status == ExecutionStatus.SUCCESS
    assert res["value"] == "hello world"


@pytest.mark.parametrize("runnable_type", ["MANAGER", "TASK"])
def test_skipped_runnable_async(test_flow, runnable_type):
    @(test_flow.manager if runnable_type == "MANAGER" else test_flow.task)
    async def simple_runnable():
        await asyncio.sleep(0.1)
        pass

    res = simple_runnable()
    assert res.status == ExecutionStatus.SKIPPED


@pytest.mark.parametrize("runnable_type", ["MANAGER", "TASK"])
def test_error_runnable_async(test_flow, runnable_type):

    @(test_flow.manager if runnable_type == "MANAGER" else test_flow.task)
    async def simple_runnable():
        await asyncio.sleep(0.1)
        raise RuntimeError("something went wrong")

    res = simple_runnable()
    assert res.status == ExecutionStatus.ERROR
    assert str(res.output) == str(RuntimeError("something went wrong"))


if __name__ == "__main__":
    test_ok_runnable()
    test_error_runnable()
    test_skipped_runnable()
    test_ok_runnable_async()
    test_error_runnable_async()
    test_skipped_runnable_async()
