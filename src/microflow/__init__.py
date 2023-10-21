import logging
import os

from microflow.flow import create_flow, Schedule
from microflow.runnable import ConcurrencyGroup
from microflow.exec_result import ExecutionResult, ExecutionStatus

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s - %(name)s(pid:{os.getpid()}) - %(levelname)s - %(message)s",
)
