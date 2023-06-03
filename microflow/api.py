import typing

from fastapi import FastAPI, Request
from starlette.applications import Starlette
from starlette.staticfiles import StaticFiles
from pydantic import BaseModel
from contextlib import asynccontextmanager
import uvicorn
import pkg_resources

if typing.TYPE_CHECKING:
    from microflow.flow import Flow


def create_app(flow: "Flow"):

    @asynccontextmanager
    async def lifespan(app: Starlette):
        await flow.init()
        yield
        await flow.shutdown()

    main_app = Starlette(lifespan=lifespan)

    api_app = FastAPI()

    api_app.state.flow = flow

    @api_app.get("/runnables")
    def get_runnables(request: Request):
        return request.app.state.flow.runnables.get_summary()

    @api_app.get("/runs")
    async def get_runs(request: Request):
        return await request.app.state.flow.event_store.get_runs()

    class RunRequest(BaseModel):
        runnable_name: str

    @api_app.post("/runs")
    def start_run(request: Request, run_request: RunRequest):
        exec_id = request.app.state.flow.start_run(runnable=run_request.runnable_name)
        return {"status": "run requested", "run_id": exec_id}

    main_app.mount("/api", api_app)
    gui_folder = pkg_resources.resource_filename(__name__, "ui-build")
    main_app.mount("/", StaticFiles(directory=gui_folder, html=True))

    return main_app


if __name__ == "__main__":
    app = create_app()
    uvicorn.run(app, host="127.0.0.1", port=3000, log_level="info")
