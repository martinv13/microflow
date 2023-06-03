import pytest
import asyncio
from microflow import create_flow, ExecutionStatus


@pytest.fixture
def test_flow():
    return create_flow("test_flow")


def test_nested_manager(test_flow):
    @test_flow.manager
    def nested_manager(a):
        return {"value": a["value"] + 1}

    @test_flow.manager
    def outer_manager():
        a = {"value": 1}
        return nested_manager(a)

    res = outer_manager()
    assert res.status == ExecutionStatus.SUCCESS
    assert res["value"] == 2


def test_nested_task(test_flow):
    @test_flow.task
    def nested_task(a):
        return {"value": a["value"] + 1}

    @test_flow.manager
    def outer_manager():
        a = {"value": 1}
        return nested_task(a)

    res = outer_manager()
    assert res.status == ExecutionStatus.SUCCESS
    assert res["value"] == 2


if __name__ == "__main__":
    test_nested_manager()
    test_nested_task()
