import importlib
import typing
import os
import sys
import datetime

from fastapi import FastAPI, Request, HTTPException
from starlette.applications import Starlette
from starlette.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import uvicorn
import pkg_resources

from microflow.run import Run

if typing.TYPE_CHECKING:
    from microflow.flow import Flow


def load_flow_from_module_path(flow_path: str) -> tuple:
    flow_path = flow_path.partition(":")
    flow_target = flow_path[len(flow_path) - 1] if len(flow_path) > 1 else "flow"
    modname = flow_path[0]
    if os.getcwd() not in sys.path:
        sys.path.append(os.getcwd())
    flow_module = importlib.import_module(modname)
    flow = getattr(flow_module, flow_target)
    return flow, flow_module, flow_target


class FlowServer:
    def __init__(self, flow_path: str = None, flow: "Flow" = None):

        if flow is not None:
            self.flow = flow
            self.flow_path = None
            self.flow_module = None
            self.flow_target = None
        else:
            self.flow_path = flow_path
            self.flow, self.flow_module, self.flow_target = load_flow_from_module_path(
                flow_path
            )

        self.app = self.create_app()

    def serve(self, host="localhost", port=3000):
        uvicorn.run(self.app, host=host, port=port, log_level="info")

    async def reload_flow(self):
        if not self.flow_module:
            raise RuntimeError(
                "reloading is possible only when app is run with flow_path"
            )
        await self.flow.shutdown()
        importlib.invalidate_caches()
        importlib.reload(self.flow_module)
        self.flow = getattr(self.flow_module, self.flow_target)
        await self.flow.init()

    def create_app(self):
        @asynccontextmanager
        async def lifespan(app: Starlette):
            await self.flow.init()
            yield
            await self.flow.shutdown()

        main_app = Starlette(lifespan=lifespan)

        api_app = FastAPI()

        api_app.state.flow_server = self

        @api_app.get("/runnables")
        def get_runnables(request: Request):
            return request.app.state.flow_server.flow.runnables.get_summary()

        @api_app.get("/runs")
        async def get_runs(
            request: Request,
            runnable_name: str | None = None,
            status: str | None = None,
            run_group: str | None = None,
            run_key: str | None = None,
            date_start: datetime.datetime | None = None,
            date_end: datetime.datetime | None = None,
            limit: int | None = None,
        ) -> list[Run]:
            return await request.app.state.flow_server.flow.event_store.get_runs(
                runnable_name=runnable_name,
                status=status,
                run_group=run_group,
                run_key=run_key,
                date_start=date_start,
                date_end=date_end,
                limit=limit,
            )

        @api_app.post("/runs")
        async def start_run(request: Request, run: Run):
            if run.runnable_name not in request.app.state.flow_server.flow.runnables:
                raise HTTPException(status_code=400, detail="Runnable name not found")
            args = run.inputs if run.inputs is not None else []
            run_id = await request.app.state.flow_server.flow.start_run(
                run.runnable_name, *args
            )
            return {"status": "run requested", "run_id": run_id}

        @api_app.post("/reload")
        async def reload_flow(request: Request):
            await request.app.state.flow_server.reload_flow()
            return {"status": "flow reloaded"}

        main_app.mount("/api", api_app)
        gui_folder = pkg_resources.resource_filename(__name__, "ui-build")
        main_app.mount("/", StaticFiles(directory=gui_folder, html=True))

        return main_app
