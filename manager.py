
import contextvars
import uuid
import asyncio

from instance import Instance

ctx_run_id = contextvars.ContextVar("manager_run_id")


class Manager:

    def __init__(self, manager_fun, **kwargs):
        self.instance = Instance()
        self.manager_fun = manager_fun

    def run_syn(self, payload, context):
        ctx_run_id.set(context["manager_run_id"])
        return self.manager_fun(payload)

    async def run(self, payload):
        context = {
            "manager_run_id": uuid.uuid4()
        }
        await asyncio.to_thread(manager.run, payload, context)




