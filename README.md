# Unified scheduling simulator

This Python 3 package contains a set of scripts to simulate the execution of different kinds of scheduling algorithms.
The simulation here means that the scheduling algorithm runs with some given inputs and provides a mapping of tasks to resources.
This mapping can be used in different ways, such as an actual mapping for a system, as a result to analyze the quality of the schedule or for comparisons, and as an input to another scheduling algorithm.

If you want to see examples of how to use this simulator, check the [examples section](#examples) below. 
If you want to understand better the code inside this package, check the [modules section](#modules).
Information about the ways we represent scheduling data are available in the [data representation section](#data).

This software is made available using the CeCILL-C license. Check our [license file](LICENSE) for more information.

## Examples

If you have issues with importing the package or its modules (such as the message `ModuleNotFoundError: No module named 'simulator'`), try running `source ./configure.sh` in this directory to update `PYTHONPATH`. 
If you are missing any packages, you can run `pip3 install -r requirements.txt` to install the required packages.

Several examples of scripts using our simulator package can be found in the [examples](examples/) directory.

**Reading an input (context):**

```python
>>> from simulator.context import Context
>>> my_input = Context.from_csv('input_examples/tiny_example.csv')
>>> my_input.tasks
OrderedDict([(0, Task (load: 1.0, mapping: 2)), (1, Task (load: 5.0, mapping: 1)), (2, Task (load: 4.0, mapping: 0)), (3, Task (load: 3.0, mapping: 1)), (4, Task (load: 2.0, mapping: 2))])
>>> my_input.resources
OrderedDict([(0, Resource (load 4.0)), (1, Resource (load 8.0)), (2, Resource (load 3.0))])
```

**Writing the context to a file:**

```python
...
>>> my_input.to_csv('myfile.csv')
```

**Running a Round-robin scheduling algorithm over the context:**

```python
...
>>> from simulator.scheduler import RoundRobin
>>> scheduler = RoundRobin(screen_verbosity=2, logging_verbosity=0)
>>> scheduler.schedule(my_input)

-- Start of experiment --
- Running scheduler RoundRobin for 5 tasks and 3 resources.
- [RNG seed (0); Bundle load limit (10); Epsilon (1.05)]
[0] -- Status:
- maximum load is 8.0
- number of overloaded, underloaded and average loaded resources are 1, 2, and 0
- number of migrations is 0
- number of load checks is 0
[0] - Task 0 (load 1.0) migrating from 2 to 0.
[0] - Task 1 (load 5.0) migrating from 1 to 1.
[0] - Task 2 (load 4.0) migrating from 0 to 2.
[0] - Task 3 (load 3.0) migrating from 1 to 0.
[0] - Task 4 (load 2.0) migrating from 2 to 1.
[1] -- Status:
- maximum load is 7.0
- number of overloaded, underloaded and average loaded resources are 1, 2, and 0
- number of migrations is 5
- number of load checks is 0
-- End of experiment --
```

This previous code also generates a file `experiment_stats.csv` with statistics from the scheduler execution.

---

## Modules

This package is organized in two main modules (*context* and *scheduler*) and four support modules (*heap*, *logger*, *resources*, and *tasks*).

### Context

A *context* represents the information used by a scheduling algorithm.
This information is mainly focused on the tasks and resources.
The *context* also interacts with a *logger* to register scheduling events and results during execution.

We have two kinds of *contexts* available for now.

- The `Context` class is mainly used by centralized schedulers. Besides reading and writing context data (`.to_csv()` and `Context.from_csv()`), it also provides some methods to compute statistics over the resources (`.avg_resource_load()` and `.max_resource_load()`) and to update the schedule (`.update_mapping()`).
- The `DistributedContext` class is mainly used by distributed schedulers that work in rounds or steps. Besides the usual `Context` methods, it also includes some to help with rounds (`.prepare_round()`) and to interact with the pseudo-random number generator (`.check_migration()`).

### Scheduler

A *scheduler* represents a scheduling algorithm. Given a set of tasks and resources (and maybe some additional information), it computes a mapping of these tasks to these resources. The main method of a *scheduler* is `schedule(context)`.
The schedulers currently available in this module are:

- **RoundRobin**: a simple, load-oblivious scheduling algorithm that follows the [round-robin policy](https://en.wikipedia.org/wiki/Round-robin_scheduling).
- **Compact**: a load-oblivious scheduling algorithm that partitions the tasks in groups of contiguous tasks with similar numbers of members, and maps the partitions to the resources in order.
- **ListScheduling**: a load-aware scheduling algorithm that follows the list scheduling idea. Tasks are taken in lexicographical order and mapped to the least loaded resources available.
- **LPT**: longest-processing time list scheduling policy. As the list scheduling algorithm, but task priorities are set based on a decreasing load order.
- **Selfish**: a distributed scheduling algorithm. On each round, we pick a resource at random for each task and decide if it migrates or not.
- **BundledSelfish**: as Selfish, but instead of working with tasks, we create bundles of tasks with a given load limit and make decisions for them.

---

## Data representation

The simulator handles and generates three kinds of files: contexts, statistics, and logs.

### Context files

A context is a CSV file organized as follows:

- Its first line contains information about the number of tasks and resources, the name of the algorithm that generated the mapping, and the random number generator seed possibly used.
- Its second line informs that the next lines contain the following information in order: task identifier, task load, and task mapping.

Example of context file:

```
# tasks:5 resources:3 rng_seed:0 algorithm:none
task_id,task_load,task_mapping
0,1.0,2
1,5.0,1
2,4.0,0
3,3.0,1
4,2.0,2
```

### Statistics files

A statistics file is a CSV file generated after running a scheduling algorithm if its screen or logging verbosity is set to one or higher.
It is organized as follows:

- Its first line contains information about the number of tasks and resources, the name of the algorithm that generated the mapping, and the random number generator seed possibly used.
- Its second line informs that the next lines contain the following information in order: round number, maximum resource load, number of overloaded resources, number of underloaded resources, number of average-loaded resources, number of tasks migrates in the round, and number of load checks in the round (mainly for distributed schedulers).
- The third line includes the previous information for round zero, aka, the situation before scheduling.
- The next lines contain these statistics for different scheduling rounds. Schedulers that do not work in rounds will only have one additional line.

A resource is considered underloaded if its load is below the average resource load. It is considered average-loaded if its load is between the average and a threshold (usually 1.05 times the average load). All other situations constitute an overloaded resource.

Example of statistics file:

```
# tasks:5 resources:3 rng_seed:0 algorithm:RoundRobin
round,maxload,overloaded,underloaded,avgloaded,migration_count,load_checks
0,8.0,1,2,0,0,0
1,7.0,1,2,0,5,0

```

### Log files

A log file contains simple text listing what happened during the execution of a scheduling algorithm. The amount of information depends on the logging verbosity set for the scheduler. A verbosity of zero means that nothing is logged. A verbosity of one means that some general information about the rounds is logged. A verbosity of two includes events such as task migrations.

The logged information is the same information that is provided in the standard output.

Example of log file generated with a verbosity level equal to two: 

```
-- Start of experiment --
- Running scheduler RoundRobin for 5 tasks and 3 resources.
- [RNG seed (0); Bundle load limit (10); Epsilon (1.05)]
[0] -- Status:
- maximum load is 8.0
- number of overloaded, underloaded and average loaded resources are 1, 2, and 0
- number of migrations is 0
- number of load checks is 0
[0] - Task 0 (load 1.0) migrating from resource 2 to 0.
[0] - Task 1 (load 5.0) migrating from resource 1 to 1.
[0] - Task 2 (load 4.0) migrating from resource 0 to 2.
[0] - Task 3 (load 3.0) migrating from resource 1 to 0.
[0] - Task 4 (load 2.0) migrating from resource 2 to 1.
[1] -- Status:
- maximum load is 7.0
- number of overloaded, underloaded and average loaded resources are 1, 2, and 0
- number of migrations is 5
- number of load checks is 0
-- End of experiment --
```
