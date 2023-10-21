import argparse
from microflow.api import FlowServer, load_flow_from_module_path


def main():
    parser = argparse.ArgumentParser(
        prog="Microflow",
        description="A tiny Python orchestrator",
    )

    subparsers = parser.add_subparsers(help="subcommand help", dest="command")

    serve_cmd = subparsers.add_parser("serve", description="Run server")
    serve_cmd.add_argument("-f", "--flow", required=True)
    serve_cmd.add_argument("-s", "--host", default="localhost")
    serve_cmd.add_argument("-p", "--port", default=3000)

    run_cmd = subparsers.add_parser("run", description="Run a task or manager")
    run_cmd.add_argument(
        "-f",
        "--flow",
        required=True,
        help="module and flow instance name (eg: path.to:the_flow)",
    )
    run_cmd.add_argument(
        "-r", "--runnable", required=True, help="name of the task or manager to run"
    )

    args = parser.parse_args()

    if args.command == "serve":
        flow_server = FlowServer(flow_path=args.flow)
        flow_server.serve(args.host, args.port)

    elif args.command == "run":
        flow, _, _ = load_flow_from_module_path(args.flow)
        flow.start_run(args.runnable, sync=True)
