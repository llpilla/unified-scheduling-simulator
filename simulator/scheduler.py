"""
Scheduler module. Contains scheduling algorithms.

Scheduling algorithms receive a context and reschedule tasks.
"""

import sys              # for sys.float_info.max
import random
import numpy as np
import copy

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


class ParametrizedLPT(Scheduler):
    """
    Largest Processing Time scheduling algorithm with extra parameters.
    Inherits from Scheduler class

    Notes
    -----
    The LPT policy takes tasks in decreasing load order and maps them
    to the least loaded resources. The parameters influence if the
    task should end up in its original resource or not.
    """
    def __init__(self,
                 load_objective=0.0,
                 task_friction=1.0,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """
        Creates an ParametrizedLPT scheduler with its verbosity

        Parameters
        ----------
        load_objective : float
            Maximum resource load that the algorithm is trying achieve.
        task_friction : float
            Factor that influences how more likely a task is to stay
            in its original resource.
        """
        Scheduler.__init__(self, name='ParametrizedLPT',
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)
        self.load_objective = load_objective
        self.task_friction = task_friction

    def run_policy(self, context):
        """
        Schedules tasks following a list scheduling policy with a
        preference to keeping tasks in their original resources.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        # Creates a list of resources with zero load
        num_resources = context.num_resources()
        res_load = [0 for i in range(num_resources)]
        # Createa a max heap of tasks
        num_tasks = context.num_tasks()
        task_heap = HeapFactory.create_loaded_heap(context.tasks, 'max')
        # Sets the current resource load objective
        load_objective = self.load_objective
        task_friction = self.task_friction
        # Iterates over tasks
        # Maps the most loaded task to the least loaded resource
        # or back to its original resource
        for i in range(num_tasks):
            task_load, task_id = task_heap.pop()
            # Gets information about the (least loaded) candidate resource
            # and the current resource of the task
            candidate_id = res_load.index(min(res_load))
            candidate_load = res_load[candidate_id]
            source_id = context.tasks[task_id].mapping
            source_load = res_load[source_id]
            # Decides if the task should stay or migrate
            if (source_load <= (candidate_load+0.01)*task_friction) and \
               (source_load + task_load <= load_objective):
                # task should stay
                chosen_resource = source_id
            else:
                # task should migrate
                chosen_resource = candidate_id
            # Updates task mapping
            context.update_mapping(task_id, chosen_resource)
            # Updates resource load information
            res_load[chosen_resource] += task_load
            # Updates the load objective if we have surpassed it
            if res_load[chosen_resource] > load_objective:
                load_objective = res_load[chosen_resource]


class GreedyRefine(Scheduler):
    """
    Largest Processing Time-based scheduling algorithm. Inherits from Scheduler
    class

    Notes
    -----
    GreedyRefine is based on a Charm++ code.
    It uses LPT to find a maximum resource load scheduling objective,
    and computes several ParametrizedLPT schedules to find the best one that
    also respects a limit in the number of migrations.
    """

    # Factor to multiply the makespan found by LPT
    load_objective_factor_values = [1.0, 1.005, 1.01, 1.015, 1.02, 1.03, 1.04,
                                    1.05, 1.06, 1.07, 1.08, 1.16, 1.2, 1.3]
    # Factor that influences how more likely a task is to stay
    # in its original resource.
    task_friction_values = [1.0, 1.05, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8,
                            1.9, 2.0, 2.1, 2.2, 2.3, sys.float_info.max]

    def __init__(self,
                 number_of_solutions=1,
                 migration_limit=1.0,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """
        Creates a GreedyRefine scheduler with its verbosity.

        Parameters
        ----------
        number_of_solutions : int (1)
            Number of ParametrizedLPT solutions to generate
        migration_limit : float (1.0)
            Fraction of tasks that can be migrated
        """
        Scheduler.__init__(self, name='GreedyRefine',
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)
        self.number_of_solutions = number_of_solutions
        self.migration_limit = migration_limit

    def run_policy(self, context):
        """
        Schedules tasks following the GreedyRefine policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        # Computes a largest processing time schedule to get a
        # makespan objective
        lpt_context = copy.deepcopy(context)
        lpt = LPT(0, 0)
        lpt.schedule(lpt_context)
        load_objective = lpt_context.max_resource_load()

        # Computes all the solutions with ParametrizedLPT
        solutions = []
        # First solution:
        # objective = load_objective
        # task_friction = -1
        solutions.append(copy.deepcopy(context))
        scheduler = ParametrizedLPT(load_objective, -1.0, 0, 0)
        scheduler.schedule(solutions[0])

        # Other solutions:
        # task_friction comes from task_friction_values
        # objective is multiplied by load_objective_factor_values
        # The algorithm tries all task friction values before moving to
        # the next objective
        to_compute = min(self.number_of_solutions,
                         len(self.load_objective_factor_values) *
                         len(self.task_friction_values))
        for i in range(1, to_compute):
            # copy of the context to compute a new solution
            solutions.append(copy.deepcopy(context))
            # task friction and load objective factor
            tf_index = (i-1) % len(self.task_friction_values)
            task_friction = self.task_friction_values[tf_index]
            lf_index = int((i-1) / len(self.task_friction_values))
            load_factor = self.load_objective_factor_values[lf_index]
            # scheduler with new parameters
            scheduler.load_objective = load_objective * load_factor
            scheduler.task_friction = task_friction
            scheduler.schedule(solutions[i])

        # Tries to find the best solution among the computed ones
        # 1. Finds the solution with the lowest number of migrations
        #    for the case where no solution is considered 'feasible'
        migrations_allowed = context.num_tasks() * self.migration_limit
        best_migrations = migrations_allowed + 1
        best_makespan = sys.float_info.max
        best_feasible_makespan = sys.float_info.max
        feasible_solutions = False
        best_solution = -1
        # Iterates over the solutions
        for solution in solutions:
            # Gets the number of migrations and makespan from the solution
            migrations = solution.num_migrations
            makespan = solution.max_resource_load()
            # Checks if the solution is feasible and better than the
            # previous feasible one
            if (migrations <= migrations_allowed) and \
               (makespan < best_feasible_makespan):
                best_feasible_makespan = makespan
                feasible_solutions = True
            # Checks if the solution is the best one found yet
            # in number of migrations
            if (migrations < best_migrations) or \
               ((migrations == best_migrations) and
               makespan < best_makespan):
                best_migrations = migrations
                best_makespan = makespan
                best_solution = solution

        # 2. If there are feasible solutions, tries to find the one
        #    with the fewest migrations and a makespan within tolerance
        if feasible_solutions is True:
            best_feasible_migrations = migrations_allowed + 1
            # tradeoff between migrations and reducing the makespan
            tradeoff_migrations = 1.003
            # Iterates over the solutions
            for solution in solutions:
                # Gets the number of migrations and makespan from the solution
                migrations = solution.num_migrations
                makespan = solution.max_resource_load()
                # Checks if this solution is feasible
                # and better than the previous one
                if ((migrations < best_feasible_migrations) and
                   (makespan <= best_feasible_makespan*tradeoff_migrations)) \
                   or ((migrations == best_feasible_migrations) and
                   (makespan < best_makespan)):
                    best_feasible_migrations = migrations
                    best_makespan = makespan
                    best_solution = solution

        # Applies the best schedule
        # (or the one with the lowest number of migrations)
        num_tasks = context.num_tasks()
        migrated_tasks = best_solution.tasks
        # Checks for all tasks if their mapping changed
        for task_id in range(num_tasks):
            context.update_mapping(task_id, migrated_tasks[task_id].mapping)


class GreedyComm(Scheduler):
    """
    Largest Processing Time-based scheduling algorithm that also considers
    communication. Inherits from Scheduler class

    Notes
    -----
    The GreedyComm policy takes tasks in decreasing load order and maps them
    to the least loaded resources or a resource that has a communicating task.
    Requires a CommunicationContext with an undirected graph.
    """

    def __init__(self,
                 msg_weight=0.001,
                 vol_weight=0.001,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a GreedyComm scheduler with its verbosity"""
        Scheduler.__init__(self, name='GreedyComm',
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)
        self.msg_weight = msg_weight
        self.vol_weight = vol_weight

    def run_policy(self, context):
        """
        Schedules tasks following a list scheduling policy.

        Parameters
        ----------
        context : Context object
            CommunicationContext to schedule
        """
        # Creates a list of resources with zero load
        num_resources = context.num_resources()
        res_load = [0 for i in range(num_resources)]
        # Creates a max heap of tasks
        num_tasks = context.num_tasks()
        task_heap = HeapFactory.create_loaded_heap(context.tasks, 'max')
        # Creates a list saying which tasks have been scheduled
        scheduled = [False for i in range(num_tasks)]

        # Iterates over tasks
        # Maps the most loaded task to the least loaded resource
        # or one that has communicating tasks
        for i in range(num_tasks):
            task_load, task_id = task_heap.pop()
            # gets a least loaded resource
            candidate_id = res_load.index(min(res_load))
            candidate_load = res_load[candidate_id]
            # gets the communication costs related to this task
            comm_costs = self.communication_cost(task_id, context.tasks,
                                                 context.graph, scheduled)
            total_comm_cost = sum(comm_costs.values())
            if candidate_id not in comm_costs:
                comm_costs[candidate_id] = 0
            # registers the best mapping so far
            best_id = candidate_id
            best_load = candidate_load + total_comm_cost - comm_costs[best_id]

            # iterates over the involved resources to see if a better
            # mapping comes from them
            for candidate_id in comm_costs:
                candidate_load = res_load[candidate_id] + \
                                 total_comm_cost - comm_costs[candidate_id]
                # if this candidate has a better estimated load, it's the best
                if candidate_load < best_load:
                    best_id = candidate_id
                    best_load = candidate_load

            # Maps the task to the best resource found
            context.update_mapping(task_id, best_id)
            scheduled[task_id] = True
            # Updates local data structures to reflect the new loads
            # 1. the load of the new task
            res_load[best_id] += task_load
            # 2. the communication cost from the tasks in other resources
            res_load[best_id] += total_comm_cost - comm_costs[best_id]
            # 3. the communication cost in other resources
            for resource_id in comm_costs:
                if resource_id != best_id:
                    res_load[resource_id] += comm_costs[resource_id]

    def communication_cost(self, task_id, tasks, graph, scheduled):
        """
        Computes the communication cost of a task related to a subset of
        resources.

        Parameters
        ----------
        task_id : int
            Task identifier
        tasks : OrderedDict of Task
            Tasks in the context
        graph : Graph
            Communication graph
        scheduler : list of bool
            Information if the tasks have been mapped already

        Returns
        -------
        dict of int keys and float values
            Communication costs for different resources
        """
        comm_costs = {}
        # Iterates over neighbors
        neighbors = graph.get_vertex(task_id).volume.keys()
        for neigh_id in neighbors:
            # if the neighbor has been mapped somewhere else
            # accumulates its communication
            if scheduled[neigh_id] is True:
                vol, msgs = graph.get_communication(task_id, neigh_id)
                resource_id = tasks[neigh_id].mapping
                cost = vol*self.vol_weight + msgs*self.msg_weight
                # Adds the communication cost to the dictionary
                if resource_id in comm_costs:
                    comm_costs[resource_id] += cost
                else:
                    comm_costs[resource_id] = cost
        # Returns the dictionary of communication costs
        return comm_costs


class Refine(Scheduler):
    """
    Refinement-based scheduling algorithm. Inherits from Scheduler class

    Notes
    -----
    The Refine policy moves tasks from overloaded to underloaded resources
    only. Given the most loaded resource, it tries to find the largest task
    it can migrate without making another resource overloaded.
    This process is repeated until no processor is seen as overloaded.
    If no solution is found, the threshold to overloaded resources is updated.
    This policy is based on the RefineLB algorithm in Charm++
    """

    def __init__(self,
                 epsilon=1.05,
                 screen_verbosity=1,
                 logging_verbosity=1,
                 file_prefix='experiment'):
        """Creates a Refine scheduler with its verbosity"""
        Scheduler.__init__(self, name='Refine',
                           epsilon=epsilon,
                           screen_verbosity=screen_verbosity,
                           logging_verbosity=logging_verbosity,
                           file_prefix=file_prefix)

    def run_policy(self, context):
        """
        Schedules tasks following a refinemenet scheduling policy.

        Parameters
        ----------
        context : Context object
            Context to schedule
        """
        # Computes the load objectives of the scheduler
        self.compute_load_thresholds(context.avg_resource_load(),
                                     context.max_resource_load())

        # Makes a work copy of the original context
        self.reset_work_context(context)

        # Tries to refine the mapping using the original threshold
        # (set in epsilon)
        done = self.refine(self.experiment_info.epsilon)
        # If no mapping that achieves the threshold was found,
        # tries a binary search to find a schedule with a larger threshold
        while done is False:
            # Checks if there is any space for searching left
            if self.max_overload - self.min_overload <= 1:
                # Nothing more to find, so the algorithm is done
                done = True
                continue
            # Makes a new copy of the original context
            self.reset_work_context(context)
            # Define the new threshold
            half_overload = int((self.max_overload + self.min_overload)/2)
            threshold = self.experiment_info.epsilon + half_overload*0.01
            # Tries to find a schedule with the new threshold
            improved = self.refine(threshold)
            # If we found a valid schedule, we decrease the maximum overload
            # If not, we increase the minimum overload
            if improved is True:
                self.max_overload = half_overload
            else:
                self.min_overload = half_overload
        # Copies the new schedule to the context
        self.copy_solution(context)

    def refine(self, threshold):
        """
        Refines a schedule based on an overload threshold.

        Parameters
        ----------
        threshold : float
            Acceptable load factor over the average load

        Returns
        -------
        bool
            True if a schedule that respects the threshold was found.
        """
        # Context being evaluated
        context = self.work_context
        success = True
        # An overloaded resource has a load over the threshold
        # An underloaded resource had under the average resource load
        avg_load = context.avg_resource_load()
        overload = avg_load * threshold
        # Sets resources as overloaded (in a max heap)
        # or underloaded (in a list)
        overloaded_resources = HeapFactory.start_heap('max')
        underloaded_resources = []
        self.classify_resources(overload,
                                avg_load,
                                overloaded_resources,
                                underloaded_resources)
        # Organizes lists of tasks per resource
        tasks_on_resource = self.organize_tasks_per_resource()
        # Main loop: iterates while there are overloaded resources
        while len(overloaded_resources) > 0:
            # Gets the most loaded resource
            donor_load, donor_id = overloaded_resources.pop()
            # Resets the variable that define which tasks will migrate
            # and where it will go
            highest_task_load = -1
            best_receiver_id = -1
            task_to_migrate = -1
            # Iterates over all underloaded resources
            # Checking if they can receive any of its tasks
            # Loops use 'reversed' due to the original set
            # implementations in RefineLB
            for receiver_id in reversed(underloaded_resources):
                receiver_load = context.resources[receiver_id].load
                for task_id in reversed(tasks_on_resource[donor_id]):
                    # Checks if the task is bigger than the previous best
                    # and if it does not make the resource overloaded
                    task_load = context.tasks[task_id].load
                    if (task_load > highest_task_load) and \
                       (task_load + receiver_load < overload):
                        # Sets this mapping as the one to do for the moment
                        highest_task_load = task_load
                        best_receiver_id = receiver_id
                        task_to_migrate = task_id
            # Checks if a task to migrate was found
            if task_to_migrate != -1:
                # 1. Migrates task to the new resource
                context.update_mapping(task_to_migrate, best_receiver_id)
                # 2. Updates the tasks on resources' list
                tasks_on_resource[donor_id].remove(task_to_migrate)
                tasks_on_resource[best_receiver_id].append(task_to_migrate)
                # 3. Checks if the donor is still overloaded
                # or even became underloaded
                donor_load = context.resources[donor_id].load
                if donor_load > overload:
                    overloaded_resources.push(donor_load, donor_id)
                elif donor_load < avg_load:
                    underloaded_resources.append(donor_id)
                # 4. Checks if the receiver is still underloaded
                receiver_load = context.resources[best_receiver_id].load
                if receiver_load > avg_load:
                    underloaded_resources.remove(best_receiver_id)
            else:
                # Did not find a task to migrate, so it will fail to find
                # a schedule that respects the overload threshold
                success = False
                break
        return success

    def classify_resources(self,
                           higher_threshold,
                           lower_threshold,
                           overloaded_resources,
                           underloaded_resources):
        """
        Classifies the resources in overloaded and underloaded sets.

        Parameters
        ----------
        higher_threshold : float
            Threshold to set a resource as overloaded
        lower_threshold : float
            Threshold to set a resource as underloaded
        overloaded_resources : MaxHeap
            Heap of overloaded resources
        underloaded_resources : list
            List of underloaded resources
        """
        context = self.work_context
        # Iterates over the resources to classify them
        for resource_id, resource in context.resources.items():
            if resource.load < lower_threshold:
                # Resource is underloaded
                underloaded_resources.append(resource_id)
            elif resource.load > higher_threshold:
                # Resource is overloaded
                overloaded_resources.push(resource.load, resource_id)

    def organize_tasks_per_resource(self):
        """
        Organizes a list of the tasks mapped to each resource.

        Returns
        -------
        list of list
            List of the tasks mapped to each resource
        """
        context = self.work_context
        num_resources = context.num_resources()
        # Creates the list of lists (each resource has a list of tasks)
        tasks_on_resource = [[] for i in range(num_resources)]
        # Iterates over the tasks adding them to the list of their resource
        for task_id, task in context.tasks.items():
            resource_id = task.mapping
            tasks_on_resource[resource_id].append(task_id)
        return tasks_on_resource

    def compute_load_thresholds(self, avg_load, max_load):
        """
        Defines the minimum and maximum load threshold for scheduling.

        Parameters
        ----------
        avg_load : float
            Average resource load
        max_load : float
            Maximum resouce load
        """
        # Current overload in the context
        overload = max_load/avg_load
        # Range used for finding an acceptable schedule through binary search
        # The '100' comes from a step of 0.01 for the considered imbalance
        self.min_overload = 0
        self.max_overload = int(1+(overload-self.experiment_info.epsilon)*100)

    def reset_work_context(self, context):
        """
        Makes a copy of original scheduling context.

        Parameters
        ----------
        context : Context object
            Original context to copy
        """
        self.work_context = copy.deepcopy(context)

    def copy_solution(self, context):
        """
        Copies the solution in the work context to the output.
        """
        num_tasks = context.num_tasks()
        original_tasks = context.tasks
        work_tasks = self.work_context.tasks
        # Checks for all tasks if their mapping changed
        for task_id in range(num_tasks):
            if original_tasks[task_id].mapping != work_tasks[task_id].mapping:
                # Applies the change if there was any
                context.update_mapping(task_id, work_tasks[task_id].mapping)


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
