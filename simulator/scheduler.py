"""
Scheduler module. Contains scheduling algorithms.

Scheduling algorithms receive a context and reschedule tasks.
"""


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
            Scheduling context to register information
        """
        context.stats.rng_seed = self.rng_seed
        context.stats.algorithm = self.name
        context.stats.report = self.report

    def schedule(self, context):
        """
        Applies the scheduling algorithm over a scheduling context.

        The Scheduler class only does basic registering operations and no
        actual scheduling.

        Parameters
        ----------
        context : Context object
            Scheduling context to schedule
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
        super().__init__(name="RoundRobin", report=report)

    def schedule(self, context):
        """
        Schedules tasks following a round-robin policy.

        Parameters
        ----------
        context : Context object
            Scheduling context to schedule
        """
        super().schedule(context)

        num_tasks = len(context.tasks)
        num_resources = len(context.resources)

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
        super().__init__(name="Compact", report=report)

    def schedule(self, context):
        """
        Schedules tasks following a compact policy.

        Parameters
        ----------
        context : Context object
            Scheduling context to schedule
        """
        super().schedule(context)

        num_tasks = len(context.tasks)
        num_resources = len(context.resources)

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
