# Microflow

  > This README presents an overview of future developments ideas for this package, many of them are not implemented yet.

Microflow is a lightweight orchestration framework to run your data pipelines or other automated jobs, written in Python. It runs primarily as a server and triggers jobs based on its internal schedules or external api calls. It features a web app which allows monitoring executions and running jobs manually, as well as a RESTful api which exposes the same features.

It aims to be minimalistic in a sense that it does not use a lot of resources and compute time on top of the actual jobs you orchestrate, and has a simple API which makes it easier to use. It works well on Unix and Windows platforms.

This page illustrates the full API of Microflow. Most features are optional and can be used together. As you can combine behaviors in a very flexible manner, this framework covers many different use cases.

## Main concepts

### Managers and tasks

Microflow defines two main units of work: "tasks" and "managers", which are created by decorating your own Python functions. A task is a Python function that will do the actual work, in a subprocess or other kind of runner. A manager is a Python function that will run in a separate thread in the main process and which is typically used to run tasks according to your application's logic. Managers can also run other managers, in a composable manner.

Because managers run in the main process, they should not do CPU-bound work, which should be delegated to tasks. Apart from this main difference, tasks and managers share many common features and API.

##### Example
```Python
# my_flow.py
from microflow import create_flow

flow = create_flow("my_flow")


@flow.task
def increment_value(a):
  return {
      "my_value": a["my_value"] + 1
  }


@flow.task
def add_inputs(a, b):
  return {
      "my_value": a["my_value"] + b["my_value"]
  }


@flow.manager
def compute(input_dict):
  a = {"my_value": 2}
  b = increment_value(input_dict)
  if b["my_value"] < 3:
      b = increment_value(b)
  return add_inputs(a, b)
```

As you have not added schedules nor ran explicitly the manager, nothing will run. However, you can see your flow and run it via the GUI:

```Bash
microflow serve -f my_flow:flow --host localhost --port 3000
```

This basic example with no practical value shows the simplest definition of tasks and manager. Two interesting points to be noted:
* when called from within managers, tasks or managers are blocking function calls, i.e. they will return only when result is available (or an error occurred), which let you write straightforward Python code on their results, as if they were regular function calls;
* input and output values for tasks and managers can only be dicts or lists of dicts, or explicit `ExecutionResult` objects (see below).

The philosophy of microflow is to handle only small objects in managers in order to limit the memory footprint of a running manager and ease communication between processes. For instance, instead of outputting full datasets, you should rather output a key to a key/value storage that will allow another task to retrieve the datasets. Microflow enforces a 32kb limit on payloads passed as inputs and outputs of tasks and managers. This limit can be configured for specific tasks, but it can be seen as a general guidance.

### Scheduling jobs

Managers and tasks can be scheduled easily using Crontab expressions. When provided with the same Schedule object instead of a Crontab string, managers or tasks will run on the same schedule. This is particularly useful for managers and tasks which declare their inputs: in that case, microflow will make sure to evaluate inputs before each task or manager (see more details in the next paragraph).

#### Example
```Python
# api_flow.py
from microflow import create_flow, Schedule
from other_module import (
  fetch_api_save_results,
  update_downstream_data,
  generate_daily_report,
)

flow = create_flow("my_flow")
api_schedule = Schedule("10 * * * *")


@flow.task(schedule=api_schedule)
def fetch_api_task():
  last_update = fetch_api_save_results()
  if last_update != flow.context.get("cursor", None):
      return {}


@flow.task(schedule=api_schedule, inputs=[fetch_api_task])
def update_downstream_task():
  update_downstream_data()
  return {}


@flow.task(schedule="0 8 * * *")
def generate_report_task():
  generate_daily_report()
  return {}
```
This example does quite a lot:
* it runs an API call every 10 minutes
* it compares the result (which could be in that case the last timestamp of updated data) with the result of the last execution (with the call to `flow.context.get("cursor", None)`):
* if the cursor value is different, the task execution is considered a success (the return value is not `None`), and we run `update_downstream_task` immediately;
* else, execution of task `fetch_api_task` returns nothing and the task is considered as "skipped": the scheduled execution stops there.
* On a different schedule, the task `generate_report_task` runs every day at 8 o'clock, independently.

### DAG execution

Managers and tasks can optionally declare other managers or tasks as inputs. As such, they can form a directed acyclic graph (DAG), which defines dependencies between tasks. If microflow detects circular dependencies between tasks or managers (i.e. a cycle in the graph of dependencies, it will not start and raise an error). When they are executed through a schedule or run_all function, Microflow will make sure declared inputs are executed before the current manager or task, while parallelizing execution as much as possible.

### Concurrency control

Managers can run tasks in parallel using `run_parallel` function, which runs specific tasks or managers concurrently, or `map_parallel`, which runs concurrently a given task or manager over a list of inputs. Concurrent execution also occurs within a declarative DAG execution, through a shared `Schedule` or an explicit call to `run_dag`. As managers runs in the main process, they run concurrently rather than in parallel; tasks, however, do run in parallel in different processes.

Microflow limits the number of tasks that can run at the same time, in a very granular way. Each task can have its own concurrency limit, and tasks can share common concurrency limits, e.g. if you want to limit access to a shared resource. By default, global maximum concurrency for tasks equals the number of cores available to Microflow.

Managers do not have by default a global concurrency limit, but concurrency limits can be configured in the same way as tasks. The reason for not limiting managers' concurrency in general is that many managers can run concurrently while waiting for their tasks to complete.

Also, batch execution of managers or tasks can be configured when mapping over a list of inputs or when launching runs over a list of partitions, through the UI or the CLI.

#### Example
```Python
# api_flow.py
from microflow import create_flow, ConcurrencyGroup
from other_module import check_api, send_email

flow = create_flow("my_flow")
api_limit = ConcurrencyGroup(max_concurrency=2)
db_limit = ConcurrencyGroup(max_concurrency=2)


@flow.task(max_concurrency=api_limit)
def check_endpoint_1():
  last_date_available = check_api(endpoint="endpoint1")
  if last_date_available != flow.context.get("cursor", None):
      return {
          "last_date_available": last_date_available
      }
 
  
@flow.task
def send_email_task(api_result):
   send_email(f"API updated; last date: {api_result['last_date_available']}")
   return {}


@flow.manager
def check_api_and_notify(schedule="*/10 * * * *"):
  return send_email_task(check_endpoint_1())
 
@flow.task(max_concurrency=[api_limit, db_limit])
def fetch_api_task():
  fetch_api_save_results()
  return {}

```

### Skipping executions

Microflow provides 2 mechanisms to prevent tasks and managers from running when it is not necessary and save time and resources: run keys and skipped execution results.

By providing a `run_key` value in its input dict (e.g. the name of a file to process), the task or manager will not run if it already ran successfully with this same `run_key` value previously, but rather return immediately a skipped execution result, containing the return value of this previous successful execution. `run_key` is enforced only when provided as a key of a dict passed as the first input of a task or manager.

By returning nothing, or an explicit `ExecutionResult` object with "skipped" status, tasks or managers will allow skipping execution of downstream tasks or managers taking this result as an input. This is the case both when downstream tasks or managers are called explicitly in the body of a manager, or implicitly through a declarative DAG execution (with `run_all` or a shared `Schedule`).

For instance, managers that check if new files or new data from an API are available can run on a frequent schedule. If they return nothing, downstream managers or tasks which do the heavy work will be skipped. 

By default, only tasks and managers with at least one input with success status and no input with error status will run. Otherwise, they will be skipped, or errored, respectively. This behavior can be configured by the `run_strategy` argument passed to tasks or managers decorators. ExecutionResult status will be checked for every input or every item of list inputs. Inputs that are not ExecutionResult but rather plain dicts will be handled just as ExecutionResult with success status.

#### Example


### Partitioned managers and tasks

Tasks and managers can optionally define two-dimensional partitions: one dimension for time partitioning and the other one for categorical partitioning. The two dimensions are optional. Partitioning will be typically used for tasks or managers which process a slice of data, be it daily batches, or a file type among similar files, etc.

Time partitions for tasks or managers can be defined with the `time_partitions` argument, which defines the time frequency. Categorical partitions are defined with the `cat_partitions` argument, which takes a fixed list of categories. A boolean function can be provided to define whether a partition should exist or not, through the `partition_filter` argument.

Partition keys will be found in the first dict input of the task or manager, as `time_partition` and `cat_partition` keys.

The execution of partitioned tasks and managers can be monitored and launched from the GUI, which will show the status for all partitions. This is why we chose to limit partitioning to two dimensions, because any other number could not be visualized easily. You can still combine more dimensions in the categorical partition dimension, using prefixes for instance.

`map_partitions` is provided as a convenience function to run concurrently a task or manager over an array of partitions (1-dimensional or 2-dimensional), with the possibility to target only missing or failed partitions. This function can be called directly from the GUI.

`time_partition` and `cat_partition` keys in the first input dict of the partionned task or manager will be populated only if they are provided explicitly by the caller, or if the task or manager is ran with `map_partitions`. Schedules will not automatically populate these values, because there may be a lot of different use cases needing different configurations. You can however check if these keys are `None` in the body of the task or the manager and compute them according to your logic. Just as `run_key`, you can set `time_partition` and `cat_partition` after execution by returning an explicit `ExecutionResult` object instead of just a dict or a list of dicts.
