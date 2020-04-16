# This code has only been tested with Python 3.
"""
Context module. Contains the representations of tasks and resources for
scheduling.

Each task has a load, and a mapping.
Each resource has a load.
The whole context contains a list of tasks, a list of resources, and some
scheduling statistics.
"""

import csv                      # for handling csv files
import copy
import random
from collections import OrderedDict


class Task:
    """
    Representation of a task for scheduling.

    Attributes
    ----------
    load : int or float
        Load of the task
    mapping : int
        Mapping of the task to a resource
    """

    def __init__(self, load=0, mapping=0):
        """Creates a task with a load and a mapping"""
        self.load = load
        self.mapping = mapping


class BundleOfTasks(Task):
    """
    Representation of a group of tasks for scheduling.

    Attributes
    ----------
    load : int or float
        Total load of the tasks (sum of loads)
    mapping : int
        Mapping of all tasks to resources
    task_ids : list of int
        List of tasks in the bundle
    """

    def __init__(self):
        """Creates an empty bundle of tasks."""
        Task.__init__(self)
        self.task_ids = []

    def set_mapping(self, resource_id):
        """Sets the mapping of the bundle of tasks."""
        self.mapping = resource_id

    def add_task(self, task_id, task):
        """
        Inserts task in the bundle.

        Parameters
        ----------
        task_id : int
            Identifier of the task
        task : Task object
        """
        self.load += task.load
        self.task_ids.append(task_id)

    def is_empty(self):
        """Checks if the bundle has no tasks"""
        return len(self.task_ids) is 0

    @staticmethod
    def check_task_loads(tasks, bundle_size):
        """
        Checks if all tasks have loads smaller than the bundle limit.

        Parameters
        ----------
        tasks : OrderedDict of Task objects
            Tasks to bundle
        bundle_size : int or float
            Load limit of bundles

        Returns
        -------
        bool
            True if all tasks have loads smaller than the limit
        """
        for task in tasks.values():
            if task.load > bundle_size:
                return False
        return True

    @staticmethod
    def create_inverse_mapping(tasks, num_resources):
        """
        Creates lists of tasks mapped per resource

        Parameters
        ----------
        tasks : OrderedDict of Task objects
            Tasks to bundle
        num_resources : int
            Number of resources

        Returns
        -------
        list of list of int
            List of resources. For each resource, a list of tasks mapped to it.
        """
        # Creates empty lists
        tasks_mapped = []
        for resource_id in range(num_resources):
            tasks_mapped.append([])
        # Fills lists in order
        for task_id, task in tasks.items():
            tasks_mapped[task.mapping].append(task_id)
        return tasks_mapped

    @staticmethod
    def create_simple_bundles(tasks, bundle_size, num_resources):
        """
        Bundles tasks based on their mapping and a bundle size (load limit).

        Parameters
        ----------
        tasks : OrderedDict of Task objects
            Tasks to bundle
        bundle_size : int or float
            Load limit of bundles
        num_resources : int
            Number of resources

        Returns
        -------
        OrderedDict of BundleOfTasks objects
        """
        # Checks if tasks are smaller than the load limit of bundles
        if BundleOfTasks.check_task_loads(tasks, bundle_size) is True:
            # Creates a list of tasks per resource
            mappings = BundleOfTasks.create_inverse_mapping(tasks,
                                                            num_resources)
            bundles = OrderedDict()
            bundle_id = 0
            # Creates bundles of tasks in order
            # Tasks are inserted in order while they respect the load limit
            for resource_id in range(num_resources):
                # Creates bundle
                bundle = BundleOfTasks()
                bundle.set_mapping(resource_id)

                # Inserts tasks mapped to the resource
                for task_id in mappings[resource_id]:
                    task = tasks[task_id]
                    # Checks if the task fits into the current bundle
                    if bundle.load + task.load > bundle_size:
                        # It does not, so we add the current full bundle
                        # to the list and start a new one
                        bundles[bundle_id] = bundle
                        bundle_id += 1
                        bundle = BundleOfTasks()
                        bundle.set_mapping(resource_id)
                    # Adds task to the bundle
                    bundle.add_task(task_id, task)

                # Checks if the last bundle of the resource has something
                if bundle.is_empty() is False:
                    bundles[bundle_id] = bundle
                    bundle_id += 1

            return bundles
        else:
            print("Could not create bundle of tasks due to their large loads")
            return None


class Resource:
    """
    Representation of a resource for scheduling.

    Attributes
    ----------
    load : int or float
        Load of the resource
    """

    def __init__(self, load=0):
        """Creates a resource with an id and a load."""
        self.load = load


class Context:
    """
    Scheduling context. Contains tasks, resources, and statistics.

    Attributes
    ----------
    tasks : OrderedDict of Task
        List of all tasks
    resources : OrderedDict of Resource
        List of all resources
    rng_seed : int
        Random number generator seed
    algorithm_name : string
        Name of scheduling algorithm
    bundle_size : int or float
        Size of bundle of tasks for bundled schedulers
    report : bool
        True if scheduling information should be reported during execution
    total_migrations : int
        Total number of tasks migrated
    round_migrations : int
        Number of migrations during a round
    """
    def __init__(self):
        """Creates an empty scheduling context"""
        self.tasks = OrderedDict()
        self.resources = OrderedDict()
        self.rng_seed = 0
        self.algorithm_name = "None"
        self.bundle_size = 0
        self.report = False
        self.total_migrations = 0
        self.round_migrations = 0


    def num_tasks(self):
        """Returns the number of tasks in the context"""
        return len(self.tasks)

    def num_resources(self):
        """Returns the number of resources in the context"""
        return len(self.resources)

    def check_consistency(self):
        """
        Checks the consistency of scheduling data.

        Returns
        -------
        bool
            True if the context is consistent, False otherwise.

        Notes
        -----
        The consistency check verifies that all task identifiers are within
        range, the number of tasks corresponds to the expect value, and that
        no tasks have negative loads.
        """
        tasks = self.tasks
        num_tasks = self.num_tasks()
        resources = self.resources
        num_resources = self.num_resources()
        # Checks the identifiers and loads of tasks
        for task_id, task in tasks.items():
            if task_id >= num_tasks:
                return False
            if task.load < 0:
                return False
            if task.mapping >= num_resources:
                return False
        for resource_id, resource in resources.items():
            if resource_id >= num_resources:
                return False
            if resource.load < 0:
                return False
        # No issues were found
        return True

    @staticmethod
    def from_csv(filename="scenario.csv"):
        """
        Imports a scheduling context from a CSV file.

        Parameters
        ----------
        filename : string
            Name of the CSV file containing the scheduling context.

        Returns
        -------
        Context object
            Scheduling context read from CSV file or empty context.

        Raises
        ------
        IOError
            If the file cannot be found or open.
        KeyError
            If the file does not contain the correct keywords (e.g., 'tasks'),
            or tasks have inconsistent identifiers or negative loads.

        Notes
        -----
        Each line of the file contains a task identifier, its load, and its
        mapping.
        A header informs the number of tasks, resources, and other information.
        """
        context = Context()
        try:
            with open(filename, 'r') as csvfile:
                # Example of the format of the first line to read
                # "# tasks:5 resources:3 rng_seed:0 algorithm:none"
                first_line = csvfile.readline()
                # We decompose the line into a dict while ignoring extremities
                first_info = dict(x.split(":")
                                  for x in first_line[2:-1].split(" "))
                # and we save each component for use
                num_resources = int(first_info["resources"])
                context.rng_seed = int(first_info["rng_seed"])
                context.algorithm_name = first_info["algorithm"]

                # After that, we read the other lines of the CSV file
                reader = csv.DictReader(csvfile)
                for line in reader:
                    # Each line generates a task
                    context.tasks[int(line['task_id'])] = Task(
                        load=float(line['task_load']),
                        mapping=int(line['task_mapping']),
                        )

                # Finally, we generate the list of resources
                # and update them based on tasks mapped to them
                for identifier in range(num_resources):
                    context.resources[identifier] = Resource(0)
                for task in context.tasks.values():
                    resource_id = task.mapping
                    context.resources[resource_id].load += task.load

        except IOError:
            print("Error: could not read file "+filename+".")
        except KeyError:
            print("Error: file "+filename+" contains non-standard" +
                  " formating or incorrect keys.")

        # Checks the context for any inconsistencies
        # If any are found, we generate an empty context
        if context.check_consistency() is False:
            context = Context()
        return context

    def to_csv(self, filename="scenario.csv"):
        """
        Exports a scheduling context to a CSV file.

        Parameters
        ----------
        filename : string
            Name of the CSV file to write.

        Raises
        ------
        IOError
            If the file cannot be open.

        Notes
        -----
        Each line of the file contains a task identifier, its load, and its
        mapping.
        A header informs the number of tasks, resources, and other information.
        """
        try:
            with open(filename, 'w') as csvfile:
                # Example of the format of the first line to write
                # "# tasks:5 resources:3 rng_seed:0 algorithm:none"
                comment = "# tasks:" + str(self.num_tasks()) + \
                          " resources:" + str(self.num_resources()) + \
                          " rng_seed:" + str(self.rng_seed) + \
                          " algorithm:" + self.algorithm_name + "\n"
                csvfile.write(comment)

                # CSV header: "task_id,task_load,task_mapping"
                csvfile.write("task_id,task_load,task_mapping\n")

                # After that, we write each line with a task
                for identifier, task in self.tasks.items():
                    line = str(identifier) + "," + \
                           str(task.load) + "," + \
                           str(task.mapping) + "\n"
                    csvfile.write(line)

        except IOError:
            print("Error: could not read file "+filename+".")

    def update_mapping(self, task_id, new_resource):
        """
        Updates the mapping of a task to a resource.

        Parameters
        ----------
        task_id : int
            Task identifier
        new_resource : int
            Resource identifier

        Notes
        -----
        If the task is mapped to a new resource, the migration counter
        is updated.
        """
        # Finds the task and its current mapping
        task = self.tasks[task_id]
        current_resource = task.mapping
        # If the task is going to migrate, update mapping and loads
        if new_resource != current_resource:
            self.resources[current_resource].load -= task.load
            self.resources[new_resource].load += task.load
            task.mapping = new_resource
            self.total_migrations += 1
            self.round_migrations += 1
        # Prints information about the new mapping
        if self.report is True:
            print(("- Task {task} (load {load})" +
                   " migrating from {old} to {new}.")
                  .format(task=str(task_id),
                          load=str(task.load),
                          old=str(current_resource),
                          new=str(new_resource),
                          ))

    def update_bundle_mapping(self, bundle_id, new_resource):
        """
        Updates the mapping of a task to a resource.

        Parameters
        ----------
        bundle_id : int
            Identifier of the bundle of tasks to migrate
        new_resource : int
            Resource identifier
        """
        bundle = self.round_tasks[bundle_id]
        bundle.set_mapping(new_resource)
        # Migrates all tasks in the bundle
        for task_id in bundle.task_ids:
            self.update_mapping(task_id, new_resource)

    def avg_resource_load(self):
        """Computes the average resource load"""
        # Computes total load
        total_load = 0
        for resource in self.resources.values():
            total_load += resource.load
        avg_load = total_load / self.num_resources()  # Computes the average
        return avg_load

    def max_resource_load(self):
        """Computes the maximum resource load"""
        max_load = 0
        for resource in self.resources.values():
            max_load = max(max_load, resource.load)
        return max_load


class DistributedContext(Context):
    """
    Scheduling context for distributed schedulers. Extends Context.

    Attributes
    ----------
    tasks : OrderedDict of Task
        List of all tasks
    resources : OrderedDict of Resource
        List of all resources
    rng_seed : int
        Random number generator seed
    algorithm_name : string
        Name of scheduling algorithm
    bundle_size : int or float
        Size of bundle of tasks for bundled schedulers
    report : bool
        True if scheduling information should be reported during execution
    total_migrations : int
        Total number of tasks migrated
    round_migrations : int
        Number of migrations during a round
    round_number : int
        Number of the round
    total_load_checks : int
        Total number of times the load of a resource is checked
    round_load_checks : int
        Number of times the load of resources is checked during a round
    round_tasks
        List of tasks for the round
    round_resources
        List of resources for the round
    avg_load : float
        Average resource load
    epsilon : float
        Accepted divergence from the average resource load for convergence
    """

    def __init__(self):
        """Creates an empty distributed scheduling context."""
        Context.__init__(self)
        self.round_number = 0
        self.total_load_checks = 0
        self.round_load_checks = 0
        self.round_tasks = []
        self.round_resources = []

    @staticmethod
    def from_context(context, epsilon=1.05):
        """
        Creates a distributed scheduling context from a basic context.

        Parameters
        ----------
        context : Context object
            Basic scheduling context
        epsilon : float, optional (standard = 1.05)
            Accepted divergence from the average resource load for convergence

        Returns
        -------
        DistributedContext object
        """
        dist_context = DistributedContext()

        dist_context.tasks = context.tasks
        dist_context.resources = context.resources
        dist_context.rng_seed = context.rng_seed
        dist_context.algorithm_name = context.algorithm_name
        dist_context.bundle_size = context.bundle_size
        dist_context.report = context.report
        dist_context.total_migrations = context.total_migrations
        dist_context.round_migrations = context.round_migrations
        dist_context.avg_load = dist_context.avg_resource_load()
        dist_context.epsilon = epsilon

        return dist_context

    @staticmethod
    def from_csv(filename="scenario.csv", epsilon=1.05):
        """
        Imports a scheduling context from a CSV file.

        Parameters
        ----------
        filename : string
            Name of the CSV file containing the scheduling context.
        epsilon : float, optional (standard = 1.05)
            Accepted divergence from the average resource load for convergence

        Returns
        -------
        DistributedContext object
            Scheduling context read from CSV file or empty context.

        Raises
        ------
        IOError
            If the file cannot be found or open.
        KeyError
            If the file does not contain the correct keywords (e.g., 'tasks'),
            or tasks have inconsistent identifiers or negative loads.

        Notes
        -----
        Each line of the file contains a task identifier, its load, and its
        mapping.
        A header informs the number of tasks, resources, and other information.
        """
        base_context = Context.from_csv(filename)
        dist_context = DistributedContext.from_context(base_context)
        return dist_context

    def has_converged(self):
        """
        Checks if the scheduler has converged.

        Convergence is defined as a situation where all resources have loads
        inferior to the average resource load times an epsilon.

        Returns
        -------
        bool
            True if the scheduler has converged, else otherwise
        """
        max_load = self.max_resource_load()
        convergence = max_load <= (self.avg_load * self.epsilon)
        return convergence

    def round_update(self):
        """Updates round number, migrations, and load checks"""
        self.round_number += 1
        self.round_migrations = 0
        self.round_load_checks = 0

    def prepare_round(self):
        """
        Prepares the context for a scheduling round.

        A simple round uses a simple copy of the tasks and resources for
        scheduling decisions.
        """
        # Updates round number, migrations and load checks
        self.round_update()
        self.round_tasks = copy.deepcopy(self.tasks)
        self.round_resources = copy.deepcopy(self.resources)

    def get_random_resource(self):
        """
        Returns a random resource from the resources available for the round

        Returns
        -------
        resource_id, Resource object
        """
        resources = self.round_resources
        # Picks a resource from the list at random
        resource_id, resource = random.choice(list(resources.items()))
        return resource_id, resource

    def check_viability(self, current_id, candidate_id):
        """
        Compares the load of the current and candidate resources.

        Parameters
        ----------
        current_id : int
            Current resource identifier
        candidate_id : int
            Candidate resource identifier

        Returns
        -------
        bool
            True if the load of the candidate resource is smaller than
            the current resource's load.
        """
        self.round_load_checks += 1  # load of the other resource is checked
        current_load = self.round_resources[current_id].load
        candidate_load = self.round_resources[candidate_id].load
        return current_load > candidate_load

    def check_migration(self, current_id, candidate_id):
        """
        Checks if a task should migrate following a simple test.

        Parameters
        ----------
        current_id : int
            Current resource identifier
        candidate_id : int
            Candidate resource identifier

        Returns
        -------
        bool
            True if the task should migrate

        Notes
        -----
        The probability to migrate is:
        1 - (load of new resource / load of current resource)
        """
        resources = self.round_resources
        # Gathers the loads of the two resources
        current_load = resources[current_id].load
        candidate_load = resources[candidate_id].load
        # Computes the probability
        probability = 1.0 - (candidate_load/current_load)
        return probability > random.random()

    """
    Methods for bundles of tasks

    """
    def prepare_round_with_bundles(self):
        """
        Prepares the context for a scheduling round.

        A simple round uses a simple copy of the tasks and resources for
        scheduling decisions.
        """
        # Updates round number, migrations and load checks
        self.round_update()
        self.round_resources = copy.deepcopy(self.resources)
        # Creates bundles of tasks only once
        if self.round_number is 1:
            self.round_tasks = BundleOfTasks.create_simple_bundles(
                self.tasks, self.bundle_size, self.num_resources())
