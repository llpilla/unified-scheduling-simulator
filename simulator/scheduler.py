"""
Scheduler module. Contains scheduling algorithms.

Scheduling algorithms receive a context and reschedule tasks.
"""

import random
from simulator.heap import HeapFactory


class Scheduler:
    """
    Base scheduling algorithm class.

    Attributes
    ----------
    name : string, optional
        Name of the scheduling algorithm
    report : bool, optional
        True if scheduling information should be reported during execution
    rng_seed : int, optional
        Random number generator seed
    """

    def __init__(self, name="empty-scheduler", report=False, rng_seed=0):
        """Creates a scheduler with its name, verbosity, and RNG seed"""
        self.name = name
        self.report = report
        self.rng_seed = rng_seed

    def __register_on_context(self, context):
        """
        Registers information about the scheduling algorithm in the context

        Parameters
        ----------
        context : Context object
            Context to register information
        """
        context.rng_seed = self.rng_seed
        context.algorithm_name = self.name
        context.report = self.report

    def schedule(self, context):
        """
        Applies the scheduling algorithm over a scheduling context.

        The Scheduler class only does basic registering operations and no
        actual scheduling.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        self.__register_on_context(context)


class RoundRobinScheduler(Scheduler):
    """
    Round-Robin scheduling algorithm. Inherits from the Scheduler class.

    Notes
    -----
    A round-robin algorithm takes tasks in [lexicographical] order and
    maps them to a list of resources.
    The first task is mapped to the first resource.
    The second task is mapped to the second resource.
    When the list of resources is exhausted, we start again taking resources
    from the start.
    """

    def __init__(self, report=False):
        """Creates a Round-Robin scheduler with its verbosity"""
        Scheduler.__init__(self, name="RoundRobin", report=report)

    def schedule(self, context):
        """
        Schedules tasks following a round-robin policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        Scheduler.schedule(self, context)

        num_tasks = context.num_tasks()
        num_resources = context.num_resources()

        # Iterates mapping tasks to resources in order
        for task_id in range(num_tasks):
            resource_id = task_id % num_resources
            context.update_mapping(task_id, resource_id)


class CompactScheduler(Scheduler):
    """
    Compact scheduling algorithm. Inherits from the Scheduler class.

    Notes
    -----
    A compact algorithm takes tasks in [lexicographical] order and
    maps them to a list of resources.
    Tasks are partitioned in contiguous groups of similar size.
    The first group is mapped to the first resource.
    The second group is mapped to the second resource.
    Etc.
    """

    def __init__(self, report=False):
        """Creates a Compact scheduler with its verbosity"""
        Scheduler.__init__(self, name="Compact", report=report)

    def schedule(self, context):
        """
        Schedules tasks following a compact policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        Scheduler.schedule(self, context)

        num_tasks = context.num_tasks()
        num_resources = context.num_resources()

        # Size of partitions
        partition_size = num_tasks // num_resources
        # Number of resources that will have +1 tasks
        leftover = num_tasks % num_resources
        # Starting task identifier
        task_id = 0

        # Iterates over the resources mapping groups of tasks to them
        for resource_id in range(num_resources):
            # Sets the actual size of the group of tasks to map
            # to this resource based on the existence of any leftover
            if leftover > 0:
                group_size = partition_size + 1
                leftover -= 1
            else:  # No more resources with +1 tasks
                group_size = partition_size

            for i in range(group_size):
                context.update_mapping(task_id, resource_id)
                task_id += 1


class ListScheduler(Scheduler):
    """
    Based list scheduling algorithm. Inherits from the Scheduler class.

    Notes
    -----
    The list scheduling algorithm takes tasks in [lexicographical] order
    and  maps them to the least loaded resources.
    """

    def __init__(self, report=False):
        """Creates a List scheduler with its verbosity"""
        Scheduler.__init__(self, name="ListScheduler", report=report)

    def schedule(self, context):
        """
        Schedules tasks following a list scheduling policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        Scheduler.schedule(self, context)
        # Creates a min heap of resources with zero load
        num_resources = context.num_resources()
        resource_heap = HeapFactory.create_unloaded_heap(num_resources, 'min')
        # Iterates over tasks mapping them to the least loaded resource
        for task_id, task in context.tasks.items():
            # Finds the least loaded resource
            resource_load, resource_id = resource_heap.pop()
            # Updates task mapping, and the resource's information in the heap
            context.update_mapping(task_id, resource_id)
            resource_load += task.load
            resource_heap.push(resource_load, resource_id)


class LPTScheduler(Scheduler):
    """
    Largest Processing Time scheduling algorithm. Inherits from Scheduler class

    Notes
    -----
    The LPT policy takes tasks in decreasing load order and maps them
    to the least loaded resources.
    """

    def __init__(self, report=False):
        """Creates a List scheduler with its verbosity"""
        Scheduler.__init__(self, name="LPTScheduler", report=report)

    def schedule(self, context):
        """
        Schedules tasks following a list scheduling policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        Scheduler.schedule(self, context)
        # Creates a min heap of resources with zero load
        num_resources = context.num_resources()
        resource_heap = HeapFactory.create_unloaded_heap(num_resources, 'min')
        # Createa a max heap of tasks
        num_tasks = context.num_tasks()
        task_heap = HeapFactory.create_loaded_heap(context.tasks, 'max')

        # Iterates over tasks
        # Maps the most loaded task to the least loaded resource
        for i in range(num_tasks):
            task_load, task_id = task_heap.pop()
            resource_load, resource_id = resource_heap.pop()
            # Updates task mapping, and the resource's information in the heap
            context.update_mapping(task_id, resource_id)
            resource_load += task_load
            resource_heap.push(resource_load, resource_id)


class DistributedScheduler(Scheduler):
    """
    Base distributed scheduling algorithm class.
    Provides methods for multiple distributed schedulers.
    Extends the Scheduler class.
    """

    def __init__(self, report=False, rng_seed=0):
        """Creates a distributed scheduler with its verbosity"""
        Scheduler.__init__(self, name="DistributedScheduler",
                           report=report, rng_seed=rng_seed)


class SelfishScheduler(DistributedScheduler):
    """
    Selfish scheduling algorithm.

    Notes
    -----
    Basic flow of a round:
    for each task in parallel
        choose a new resource at random
        if the load of the current resource > new resource
            migrate with a certain probability
    """

    def __init__(self, report=False, rng_seed=0):
        Scheduler.__init__(self, name="Selfish",
                           report=report, rng_seed=rng_seed)

    def schedule(self, context):
        """
        Schedules tasks following a list scheduling policy.

        Parameters
        ----------
        context : DistributedContext object
            Context to schedule
        """
        Scheduler.schedule(self, context)

        # Sets RNG seed before starting to schedule
        random.seed(self.rng_seed)

        print("Before rounds")
        while context.has_converged() is False:
            context.prepare_simple_round()
            print("Round: " + str(context.round_number))
            tasks = context.round_tasks
            # Iterates while there are tasks in the round to check
            while len(tasks) is not 0:
                task_id, task = tasks.popitem(False)
                resource_id, resource = context.get_random_resource()
                viability = context.check_simple_viability(task.mapping,
                                                           resource_id)
                if viability is True:
                    context.try_simple_migration(task_id,
                                                 task.mapping,
                                                 resource_id)




