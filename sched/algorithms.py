"""
Scheduling algorithms module. Each function represents a scheduling algorithm.

Scheduling algorithms implemented here will be found by their name
and used for mapping tasks to machines in other modules.

This module serves as a space for the implementation of new and different
scheduling algorithms.

Routines
--------

- roundrobin: a simple algorithm that maps tasks to machines one at a time
without considering their load.
- listscheduling: a list scheduling algorithm that takes a list of tasks
and maps them iteratively (one at a time) to the least loaded machines.
"""

import heapq        # For heap functionalities

def roundrobin(tasks, machines):
    """Simple load-unaware round robin algorithm"""
    mapping = [-1] * tasks.num
    for task in range(tasks.num):
        mapping[task] = task % machines.num
    return mapping

def listscheduling(tasks, machines):
    """Simple load-aware list scheduling algorithm"""
    minheap = []
    # Constructs the min heap in order
    for machine in range(machines.num):
        heapq.heappush(minheap, (0, machine)) # 0 load, machine ID

    # Gets the mapping of the tasks in list order
    mapping = [-1] * tasks.num
    for task in range(tasks.num):
        # Gets the current PE with the minimum load and its load
        machine_load, machine = heapq.heappop(minheap)
        mapping[task] = machine # sets the mapping
        machine_load += tasks.loads[task] # adds the load of the task to the PE
        heapq.heappush(minheap, (machine_load, machine))
    return mapping
