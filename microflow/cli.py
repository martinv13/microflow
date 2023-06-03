import sys
import argparse
import importlib.util


def main():
    parser = argparse.ArgumentParser(
        prog="Microflow",
        description="A tiny orchestration tool",
    )
    parser.add_argument("flow")
    parser.add_argument("-s", "--host", default="localhost")
    parser.add_argument("-p", "--port", default=3000)

    args = parser.parse_args()

    flow = args.flow.partition(":")
    target = flow[len(flow) - 1] if len(flow) > 1 else "flow"
    fname = f"{flow[0]}.py"
    modname = "user_flow"

    spec = importlib.util.spec_from_file_location(modname, fname)
    if spec is None:
        raise ImportError(f"Could not load spec for module '{modname}' at: {fname}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[modname] = module

    try:
        spec.loader.exec_module(module)
    except FileNotFoundError as e:
        raise ImportError(f"{e.strerror}: {fname}") from e

    flow = getattr(module, target)

    flow.serve(args.host, args.port)
