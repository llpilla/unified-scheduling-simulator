"""
Scheduler module. Contains scheduling algorithms.

Scheduling algorithms receive a context and reschedule tasks.
"""

import random
import numpy as np

from simulator.heap import HeapFactory
from simulator.context import ExperimentInformation


class Scheduler:
    """
    Base scheduling algorithm class.

    Attributes
    ----------
    experiment_info : ExperimentInformation object
        Basic information about the experiment
    screen_verbosity : int
        Level of information to be reported during execution
    logging_verbosity : int
        Level of information to be stored during execution
    file_prefix: string
        Prefix for the name of output files (log and stats)
    """

    def __init__(self,
                 name='empty-scheduler',
                 rng_seed=0,
                 bundle_load_limit=10,
                 epsilon=1.05,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a scheduler with its name, verbosity, and RNG seed"""
        self.experiment_info = ExperimentInformation(
            algorithm=name, rng_seed=rng_seed,
            bundle_load_limit=bundle_load_limit, epsilon=epsilon)
        self.screen_verbosity = screen_verbosity
        self.logging_verbosity = logging_verbosity
        self.file_prefix = file_prefix

    def register_start(self, context):
        """
        Registers information about the scheduling algorithm in the context

        Parameters
        ----------
        context : Context object
            Context to register information
        """
        # Updates information in the context
        context.experiment_info.update_from_scheduler(self.experiment_info)
        # Passes logging information to the context
        context.set_verbosity(self.screen_verbosity,
                              self.logging_verbosity,
                              self.file_prefix)

    def register_end(self, context):
        """
        Registers the end of scheduling in the context

        Parameters
        ----------
        context : Context object
            Context to register information
        """
        context.log_finish()

    def schedule(self, context):
        """
        Schedules tasks following the internal policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        self.register_start(context)
        # Sets RNG seed before starting to schedule
        random.seed(self.experiment_info.rng_seed)
        self.run_policy(context)
        self.register_end(context)

    def run_policy(self, context):
        pass


class RoundRobin(Scheduler):
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

    def __init__(self,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a Round-Robin scheduler with its verbosity"""
        Scheduler.__init__(self, name='RoundRobin',
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a round-robin policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        num_tasks = context.num_tasks()
        num_resources = context.num_resources()

        # Iterates mapping tasks to resources in order
        for task_id in range(num_tasks):
            resource_id = task_id % num_resources
            context.update_mapping(task_id, resource_id)


class Random(Scheduler):
    """
    Random scheduling algorithm. Inherits from the Scheduler class.

    Notes
    -----
    The random algorithm chooses resources uniformly at random for each task.
    """

    def __init__(self,
                 rng_seed=0,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a Random scheduler with its verbosity"""
        Scheduler.__init__(self, name='Random',
                           rng_seed=rng_seed,
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a random policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        num_tasks = context.num_tasks()
        num_resources = context.num_resources()
        # Iterates mapping tasks to resources in order
        for task_id in range(num_tasks):
            resource_id = random.randrange(num_resources)
            context.update_mapping(task_id, resource_id)


class RandomNormal(Scheduler):
    """
    Random scheduling algorithm using a normal distribution.
    Inherits from the Scheduler class.

    Notes
    -----
    The random algorithm chooses resources at random (from a normal
    distribution) for each task.
    """

    def __init__(self,
                 rng_seed=0,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a RandomNormal scheduler with its verbosity"""
        Scheduler.__init__(self, name='RandomNormal',
                           rng_seed=rng_seed,
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a random policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        num_tasks = context.num_tasks()
        num_resources = context.num_resources()
        # Creates a histogram to find the weights (probability)
        # of mapping tasks to any resource
        # - gets 10000 samples
        samples = [random.gauss(0.0, 1.0) for i in range(10000)]
        # - constructs a histogram with them
        limit = max(-min(samples), max(samples))
        bins = np.linspace(-limit, limit, num_resources + 1)
        histogram, bins = np.histogram(samples, bins=bins, density=False)
        # - uses the histogram as weights for random choices
        mappings = random.choices(list(range(num_resources)),
                                  weights=histogram,
                                  k=num_tasks)
        # Iterates mapping tasks to resources in order
        for task_id in range(num_tasks):
            context.update_mapping(task_id, mappings[task_id])


class RandomExponential(Scheduler):
    """
    Random scheduling algorithm using an exponential distribution.
    Inherits from the Scheduler class.

    Notes
    -----
    The random algorithm chooses resources at random (from an exponential
    distribution) for each task.
    """

    def __init__(self,
                 rng_seed=0,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a RandomExponential scheduler with its verbosity"""
        Scheduler.__init__(self, name='RandomExponential',
                           rng_seed=rng_seed,
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a random policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        num_tasks = context.num_tasks()
        num_resources = context.num_resources()
        # Creates a histogram to find the weights (probability)
        # of mapping tasks to any resource
        # - gets 10000 samples
        samples = [random.expovariate(1.0) for i in range(10000)]
        # - constructs a histogram with them
        bins = np.linspace(0, max(samples), num_resources + 1)
        histogram, bins = np.histogram(samples, bins=bins, density=False)
        # - uses the histogram as weights for random choices
        mappings = random.choices(list(range(num_resources)),
                                  weights=histogram,
                                  k=num_tasks)
        # Iterates mapping tasks to resources in order
        for task_id in range(num_tasks):
            context.update_mapping(task_id, mappings[task_id])


class Compact(Scheduler):
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

    def __init__(self,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a Compact scheduler with its verbosity"""
        Scheduler.__init__(self, name='Compact',
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a compact policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
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

    def __init__(self,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a List scheduler with its verbosity"""
        Scheduler.__init__(self, name='ListScheduler',
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a list scheduling policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
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


class LPT(Scheduler):
    """
    Largest Processing Time scheduling algorithm. Inherits from Scheduler class

    Notes
    -----
    The LPT policy takes tasks in decreasing load order and maps them
    to the least loaded resources.
    """

    def __init__(self,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates an LPT scheduler with its verbosity"""
        Scheduler.__init__(self, name='LPT',
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a list scheduling policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
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


class DistScheduler(Scheduler):
    """
    Base distributed scheduling algorithm class.
    Provides methods for multiple distributed schedulers.
    Extends the Scheduler class.

    Attributes
    ----------
    name : string, optional
        Name of the scheduling algorithm
    report : bool, optional
        True if scheduling information should be reported during execution
    rng_seed : int, optional
        Random number generator seed
    """

    def __init__(self,
                 name='DistScheduler',
                 rng_seed=0,
                 bundle_load_limit=10,
                 epsilon=1.05,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a distributed scheduler with its verbosity"""
        Scheduler.__init__(self, name=name, rng_seed=rng_seed,
                           bundle_load_limit=bundle_load_limit,
                           epsilon=epsilon,
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a distributed scheduling algorithm.

        Parameters
        ----------
        context : DistributedContext object
            Context to schedule
        """
        while self.has_converged(context) is False:
            self.prepare_round(context)
            tasks = context.round_tasks
            # Iterates while there are tasks in the round to check
            for task_id, task in tasks.items():
                resource_id, resource = self.get_candidate_resource(context)
                decision = self.check_migration(context, task_id,
                                                task.mapping, resource_id)
                if decision is True:
                    self.apply_migration(context, task_id, resource_id)

    """
    Simple set of distributed scheduling methods.
    Used by the Selfish algorith.

    """
    @staticmethod
    def basic_convergence_check(context):
        return context.has_converged()

    @staticmethod
    def basic_round(context):
        return context.prepare_round()

    @staticmethod
    def basic_resource_selection(context):
        return context.get_random_resource()

    @staticmethod
    def basic_migration_check(context, task_id, current_id, candidate_id):
        viability = context.check_viability(current_id, candidate_id)
        if viability is True:
            return context.check_migration(current_id, candidate_id)

    @staticmethod
    def apply_single_migration(context, task_id, candidate_id):
        context.update_mapping(task_id, candidate_id)

    """
    Simple set of methods for distributed schedulers that organize
    tasks in bundles (packs)
    """
    @staticmethod
    def basic_round_bundled(context):
        return context.prepare_round_bundled()

    @staticmethod
    def apply_multiple_migrations(context, bundle_id, candidate_id):
        context.update_mapping_bundled(bundle_id, candidate_id)


class Selfish(DistScheduler):
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

    def __init__(self,
                 rng_seed=0,
                 epsilon=1.05,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a Selfish scheduler"""
        DistScheduler.__init__(self, name='Selfish', rng_seed=rng_seed,
                               epsilon=epsilon,
                               screen_verbosity=screen_verbosity,
                               logging_verbosity=logging_verbosity,
                               file_prefix=file_prefix)
        # Defines the methods to be used for scheduling
        self.has_converged = DistScheduler.basic_convergence_check
        self.prepare_round = DistScheduler.basic_round
        self.get_candidate_resource = DistScheduler.basic_resource_selection
        self.check_migration = DistScheduler.basic_migration_check
        self.apply_migration = DistScheduler.apply_single_migration


class BundledSelfish(DistScheduler):
    """
    Bundled selfish scheduling algorithm.

    Notes
    -----
    Basic flow of a round:
    for each bundle of tasks in parallel
        choose a new resource at random
        if the load of the current resource > new resource
            migrate with a certain probability
    """

    def __init__(self,
                 rng_seed=0,
                 bundle_load_limit=10,
                 epsilon=1.05,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a Selfish scheduler with bundled tasks"""
        DistScheduler.__init__(self, name='BundledSelfish',
                               rng_seed=rng_seed,
                               bundle_load_limit=bundle_load_limit,
                               epsilon=epsilon,
                               screen_verbosity=screen_verbosity,
                               logging_verbosity=logging_verbosity,
                               file_prefix=file_prefix)
        # Defines the methods to be used for scheduling
        self.has_converged = DistScheduler.basic_convergence_check
        self.prepare_round = DistScheduler.basic_round_bundled
        self.get_candidate_resource = DistScheduler.basic_resource_selection
        self.check_migration = DistScheduler.basic_migration_check
        self.apply_migration = DistScheduler.apply_multiple_migrations
