import asyncio
import os
import signal
import subprocess
import sys
import abc
from typing import AsyncIterable


class BaseCLIRunner:
    @abc.abstractmethod
    async def run_command(self, cmd: str, cwd: str = None) -> AsyncIterable:
        yield None

    @abc.abstractmethod
    def terminate_run(self, force: bool = False):
        pass


class CLIRunner(BaseCLIRunner):
    def __init__(self):
        self.proc = None
        self.loop = None

    async def run_command(self, cmd, cwd=None) -> AsyncIterable:
        def runner(loop, queue, cmd):
            try:
                self.proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                    cwd=os.getcwd() if cwd is None else cwd,
                )
            except Exception as e:
                asyncio.run_coroutine_threadsafe(queue.put(e), loop)
                return

            while True:
                output = self.proc.stdout.readline().rstrip()
                if output == "" and self.proc.poll() is not None:
                    break
                if output:
                    asyncio.run_coroutine_threadsafe(queue.put(output), loop)

            if self.proc.poll() != 0:
                asyncio.run_coroutine_threadsafe(
                    queue.put(RuntimeError("cmd invocation failed")), loop
                )

            asyncio.run_coroutine_threadsafe(queue.put("CMD_RUN_STOP"), loop)

        loop = asyncio.get_running_loop()
        results_queue = asyncio.Queue()
        cmd_run_task = asyncio.create_task(
            asyncio.to_thread(runner, loop, results_queue, cmd)
        )
        while True:
            next_item = await results_queue.get()
            if isinstance(next_item, Exception):
                raise next_item
            if next_item == "CMD_RUN_STOP":
                break
            yield next_item
        await cmd_run_task

    def terminate_run(self, force=False):
        if self.proc is None:
            raise RuntimeError("trying to terminate a run which has not started")
        if self.proc.poll() is None:
            if force:
                self.proc.terminate()
            else:
                if sys.platform in ["win32", "cygwin"]:
                    self.proc.send_signal(signal.CTRL_C_EVENT)
                else:
                    self.proc.kill()


class TestCLIRunnerDBT(BaseCLIRunner):
    async def run_command(self, cmd: str, cwd: str = None) -> AsyncIterable:
        output = [
            "15:03:45  Running with dbt=1.4.6",
            "15:03:45  Unable to do partial parsing because config vars, config profile or config target have changed",
            "15:03:50  Found 101 models, 24 tests, 0 snapshots, 0 analyses, 402 macros, 0 operations, 8 seed files, 96 sources, 0 exposures, 0 metrics",
            "15:03:50  ",
            "15:03:51  Concurrency: 1 threads (target='dev')",
            "15:03:51  ",
            "15:03:51  1 of 2 START sql table model dbt_xxx_analytics.my_model_1 ............ [RUN]",
            "15:03:51  2 of 2 START sql table model dbt_xxx_analytics.my_model_2 ............ [RUN]",
            "15:03:52  2 of 2 OK created sql table model dbt_xxx_analytics.my_model_2 ....... [OK in 1.01s]",
            "15:03:54  1 of 2 OK created sql table model dbt_xxx_analytics.my_model_1 ....... [OK in 2.65s]",
            "15:03:54  ",
            "15:03:54  Finished running 2 table model in 0 hours 0 minutes and 3.50 seconds (3.50s).",
            "15:03:54  ",
            "15:03:54  Completed successfully",
            "15:03:54  ",
            "15:03:54  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2",
        ]
        for line in output:
            await asyncio.sleep(0.01)
            yield line

    async def terminate_run(self, force: bool = False):
        pass


class DBTcli:
    def __init__(self, runner: BaseCLIRunner, project_dir: str):
        self.runner = runner
        self.status = 0
        self.proc = None
        self.loop = None
        self.lock = asyncio.Lock()
        self.project_dir = project_dir

    async def run_command(self, command, **kwargs):
        async with self.lock:
            cmd = f"dbt {command} " + " ".join(
                [f"--{key.replace('_', '-')} {arg}" for key, arg in kwargs.items()]
            )
            async for line in self.runner.run_command(cmd, self.project_dir):
                yield line


if __name__ == "__main__":

    async def main():
        cli = DBTcli(TestCLIRunnerDBT(), "D:\\code\\dbt-transform")
        async for output in cli.run_command("compile"):
            print(f"line output: {output}")

    asyncio.run(main())
