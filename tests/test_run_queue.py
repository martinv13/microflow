import asyncio
import datetime

import pytest

from microflow.run_queue import RunQueue, ConcurrencyGroup


class Events:

    def __init__(self):
        self.events = []

    def add(self, event):
        self.events.append((datetime.datetime.now(), event))

    def __repr__(self):
        ref = self.events[0][0] if len(self.events) > 0 else None
        return "\n".join([
            f"{(timestamp - ref).seconds} - {event}"
            for timestamp, event in self.events
        ])


@pytest.mark.asyncio
async def test_run_sequence():
    queue = RunQueue()
    events = Events()

    def make_job(i):
        async def job():
            events.add(f"job {i} starts")
            await asyncio.sleep(1)
            events.add(f"job {i} done")
        return job

    async def with_wait(job, run_id, concurrency_group):
        async with queue.wait_in_queue(run_id, concurrency_group):
            await job()

    two = ConcurrencyGroup(2)
    single = ConcurrencyGroup(1)

    job1 = make_job(1)

    await asyncio.gather(
        with_wait(job1, "run1", two),
        with_wait(make_job(2), "run2", two),
        with_wait(make_job(3), "run3", [two, single]),
        with_wait(make_job(4), "run4", single),
    )

    print("\n")
    print(events)

