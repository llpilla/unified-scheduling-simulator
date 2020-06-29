"""
Context module. Groups information about tasks, resources, and scheduling
statistics.
"""

import csv                      # for handling csv files
import copy                     # for deep copy
import random
from collections import OrderedDict

from simulator.tasks import Task, TaskBundle
from simulator.resources import Resource
from simulator.logger import Logger


class ExperimentInformation:
    """
    Basic information about an experiment.

    Attributes
    ----------
    num_tasks : int
        Number of tasks
    num_resources : int
        Number of resources
    algorithm : string
        Scheduling algorithm name
    rng_seed : int
        Random number generator seed
    bundle_load_limit : int or float, optional
        Size of bundle of tasks for bundled schedulers
    epsilon : float
        Accepted divergence from the average resource load for convergence
    """

    def __init__(self, num_tasks=0, num_resources=0,
                 algorithm='None', rng_seed=0,
                 bundle_load_limit=10, epsilon=1.05):
        self.num_tasks = num_tasks
        self.num_resources = num_resources
        self.algorithm = algorithm
        self.rng_seed = rng_seed
        self.bundle_load_limit = bundle_load_limit
        self.epsilon = epsilon

    def __repr__(self):
        text = (f'Experiment information ({self.num_tasks},' +
                f' {self.num_resources}, {self.algorithm},' +
                f' {self.rng_seed}, {self.bundle_load_limit},' +
                f' {self.epsilon})')
        return text

    def __str__(self):
        # "# tasks:5 resources:3 rng_seed:0 algorithm:None"
        text = (f'# tasks:{self.num_tasks} resources:{self.num_resources}' +
                f' rng_seed:{self.rng_seed} algorithm:{self.algorithm}')
        return text

    def update_from_scheduler(self, new_info):
        """
        Updates all information except number of tasks and resources.

        Parameters
        ----------
        new_info : ExperimentInformation object
            Information from the scheduler
        """
        self.algorithm = new_info.algorithm
        self.rng_seed = new_info.rng_seed
        self.bundle_load_limit = new_info.bundle_load_limit
        self.epsilon = new_info.epsilon


class ExperimentStatus:
    """
    Information about the current status of a simulation.

    Attributes
    ----------
    max_resource_load : int or float
        Maximum resource load
    num_overloaded : int
        Number of overloaded resources
    num_underloaded : int
        Number of underloaded resources
    num_averageloaded : int
        Number of resources near the average load
    """

    def __init__(self, max_resource_load=0, num_overloaded=0,
                 num_underloaded=0, num_averageloaded=0):
        self.max_resource_load = max_resource_load
        self.num_overloaded = num_overloaded
        self.num_underloaded = num_underloaded
        self.num_averageloaded = num_averageloaded

    def __repr__(self):
        text = (f'Experiment status ({self.max_resource_load},' +
                f' {self.num_overloaded}, {self.num_underloaded},' +
                f' {self.num_averageloaded})')
        return text

    def __str__(self):
        text = (f'{self.max_resource_load},{self.num_overloaded},' +
                f'{self.num_underloaded},{self.num_averageloaded}')
        return text


class Context:
    """
    Scheduling context. Contains tasks, resources, and statistics.

    Attributes
    ----------
    tasks : OrderedDict of Task objects
        List of all tasks
    resources : OrderedDict of Resource objects
        List of all resources
    experiment_info : ExperimentInformation object
        Basic information about the experiment
    logging : bool
        True if logging the scheduler execution
    logger : Logger object
        Stores the log of execution
    round_tasks
        List of tasks for the round
    round_resources
        List of resources for the round
    """
    def __init__(self):
        """Creates an empty scheduling context."""
        self.tasks = OrderedDict()
        self.resources = OrderedDict()
        self.experiment_info = ExperimentInformation()
        self.logging = False
        self.logger = None
        self.round_tasks = []
        self.round_resources = []

    """
    Basic methods: gather information, check, classify

    """
    def num_tasks(self):
        """Returns the number of tasks in the context."""
        return self.experiment_info.num_tasks

    def num_resources(self):
        """Returns the number of resources in the context."""
        return self.experiment_info.num_resources

    def avg_resource_load(self):
        """Computes the average resource load"""
        # Computes total load
        total_load = 0
        avg_load = 0
        if self.num_resources() > 0:  # Prevents a division by zero
            for resource in self.resources.values():
                total_load += resource.load
            avg_load = total_load / self.num_resources()  # Computes average
        return avg_load

    def max_resource_load(self):
        """Computes the maximum resource load"""
        max_load = 0
        for resource in self.resources.values():
            max_load = max(max_load, resource.load)
        return max_load

    def gather_status(self):
        """
        Organizes the current status of the simulation.

        Returns
        -------
        ExperimentStatus object
        """
        max_resource_load = self.max_resource_load()
        overloaded, underloaded, averageloaded = self.classify_resources()
        status = ExperimentStatus(max_resource_load, overloaded,
                                  underloaded, averageloaded)
        return status

    def classify_resources(self):
        """
        Counts and classifies the resources according to their loads.

        Returns
        -------
        int, int, int
            number of overloaded, underloaded, and average-loaded resources
        """
        overloaded = underloaded = averageloaded = 0
        avg_load = self.avg_resource_load()
        threshold_load = avg_load * self.experiment_info.epsilon
        for resource in self.resources.values():
            if resource.load < avg_load:
                underloaded += 1
            elif resource.load <= threshold_load:
                averageloaded += 1
            else:
                overloaded += 1
        return overloaded, underloaded, averageloaded

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
        # Checks the number of tasks and resources
        tasks = self.tasks
        num_tasks = self.num_tasks()
        if num_tasks != self.experiment_info.num_tasks:
            return False
        resources = self.resources
        num_resources = self.num_resources()
        if num_resources != self.experiment_info.num_resources:
            return False
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

    """
    Creation methods: read and write a context from a CSV file
    or from a list of task loads
    """
    @staticmethod
    def from_loads(task_loads, num_resources=2, rng_seed=0, name='from_loads'):
        """
        Creates a scheduling context from a list of task loads and
        a number of resources.

        All tasks are initially mapped to resource zero.

        Parameters
        ----------
        task_loads : list of int or float
            Loads of tasks.
        num_resources : int
            Number of resources in the context.
        rng_seed : int, optional
            Random number generator seed used previously.
        name : string
            Name of the method used for the creation of the context.

        Returns
        -------
        Context object
            Scheduling context based on the list of loads, or empty context.
        """
        context = Context()  # empty context
        # Starts adding resources to the context
        for identifier in range(num_resources):
            context.resources[identifier] = Resource(0)
        # Starts adding tasks to the context
        for identifier in range(len(task_loads)):
            task = Task(task_loads[identifier], 0)
            context.tasks[identifier] = task
            # Adds the load of the task to the initial resource
            context.resources[0].load += task_loads[identifier]
        # Updates experiment information
        context.experiment_info.num_tasks = len(task_loads)
        context.experiment_info.num_resources = num_resources
        context.experiment_info.rng_seed = rng_seed
        context.experiment_info.algorithm = name

        # Checks the context for any inconsistencies
        # If any are found, we generate an empty context
        if context.check_consistency() is False:
            context = Context()
        return context

    @staticmethod
    def from_csv(filename='scenario.csv'):
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
                first_info = dict(x.split(':')
                                  for x in first_line[2:-1].split(' '))
                # and we save each component for use
                num_tasks = int(first_info['tasks'])
                num_resources = int(first_info['resources'])
                algorithm = first_info['algorithm']
                rng_seed = int(first_info['rng_seed'])
                experiment = ExperimentInformation(num_tasks, num_resources,
                                                   algorithm, rng_seed)
                context.experiment_info = experiment

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
            print('Error: could not read file '+filename+'.')
        except KeyError:
            print('Error: file '+filename+' contains non-standard' +
                  ' formating or incorrect keys.')

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
                csvfile.write(str(self.experiment_info) + '\n')

                # CSV header: "task_id,task_load,task_mapping"
                csvfile.write('task_id,task_load,task_mapping\n')

                # After that, we write each line with a task
                for identifier, task in self.tasks.items():
                    line = (str(identifier) + ',' +
                            str(task.load) + ',' +
                            str(task.mapping) + '\n')
                    csvfile.write(line)

        except IOError:
            print('Error: could not read file '+filename+'.')

    """
    Log methods: start and stop logging an experiment
    """
    def set_verbosity(self, screen_verbosity=1, logging_verbosity=1,
                      file_prefix='experiment'):
        """
        Sets the level of information to be printed and logged during
        execution. Starts the logging process.

        Parameters
        ----------
        screen_verbosity : int, optional (standard = 1)
            Level of information to be reported during execution
        logging_verbosity : int, optional (standard = 1)
            Level of information to be stored during execution
        file_prefix : string, optional (standard = 'experiment')
            Prefix for the name of output files (log and stats)
        Notes
        -----
        Verbosity levels:
        0 - nothing is reported
        1 - basic statistics are reported (number of tasks & resources, loads)
        2 - every action is reported
        """
        # If we have to log something, we start our logger
        if screen_verbosity > 0 or logging_verbosity > 0:
            self.logging = True
            self.logger = Logger(screen_verbosity,
                                 logging_verbosity,
                                 file_prefix)
            self.log_start()

    def log_start(self):
        """
        Starts headers in the logger.

        Notes
        -----
        Information that is provided:
        - number of tasks, number of resources, RNG seed, scheduler's name
        - maximum resource load
        - number of resources that are overloaded, underloaded, or near the
        average load
        """
        if self.logging is True:
            # first batch of information
            self.logger.register_start(self.experiment_info)
            # second batch of information
            experiment_status = self.gather_status()
            self.logger.register_resource_status(experiment_status)

    def log_finish(self):
        """
        Stops logging an experiment.
        """
        if self.logging is True:
            experiment_status = self.gather_status()
            self.logger.num_round += 1
            self.logger.register_resource_status(experiment_status)
            # stops logging
            self.logger.register_end()

    """
    Mapping methods: update mapping of tasks in the context
    """
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
            # Only logs migrations to different resources
            if self.logging is True:
                self.logger.register_migration(task_id, task.load,
                                               current_resource, new_resource)

    def update_mapping_bundled(self, bundle_id, new_resource):
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


class DistributedContext(Context):
    """
    Scheduling context for distributed schedulers. Extends Context.

    Attributes
    ----------
    tasks : OrderedDict of Task
        List of all tasks
    resources : OrderedDict of Resource
        List of all resources
    avg_load : float
        Average resource load
    experiment_info : ExperimentInformation object
        Basic information about the experiment
    logging : bool
        True if logging the scheduler execution
    logger : Logger object
        Stores the log of execution
    round_tasks
        List of tasks for the round
    round_resources
        State of all resources for the round
    round_targets
        List of resources that are candidates to receive tasks
        during the round
    """

    def __init__(self):
        """Creates an empty distributed scheduling context."""
        Context.__init__(self)
        self.avg_load = 0

    @staticmethod
    def from_context(context):
        """
        Creates a distributed scheduling context from a basic context.

        Parameters
        ----------
        context : Context object
            Basic scheduling context

        Returns
        -------
        DistributedContext object
        """
        dist_context = DistributedContext()

        dist_context.tasks = context.tasks
        dist_context.resources = context.resources
        dist_context.round_targets = dist_context.round_resources
        dist_context.experiment_info = context.experiment_info
        dist_context.avg_load = dist_context.avg_resource_load()
        dist_context.logging = context.logging
        dist_context.logger = context.logger

        return dist_context

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
        threshold_load = self.avg_load * self.experiment_info.epsilon
        convergence = max_load <= threshold_load

        if self.logging is True:
            self.logger.register_convergence(convergence, max_load,
                                             threshold_load)

        return convergence

    def is_resource_under_threshold(self, resource_id, threshold):
        """
        Checks if a resource is under the average load.

        Parameters
        ----------
        resource_id: int
            Identifier of a resource

        Returns
        -------
        bool
            True if the resource's load is under the threshold
        """
        resource = self.resources[resource_id]
        return resource.load < threshold

    """
    Methods for schedulers: prepare rounds, get resources, check
    migration viability
    """
    def log_round(self):
        """
        Handles the logging part of a round preparation.
        """
        if self.logging is True:
            # registers information from last round
            experiment_status = self.gather_status()
            self.logger.register_resource_status(experiment_status)
            # starts new round
            self.logger.register_new_round()

    def set_tasks_with_threshold(self, threshold):
        """
        Sets the tasks for the round as the tasks from resources that
        surpass a threshold.

        Parameters
        ----------
        threshold : float
            Load that a resource must surpass to add tasks to the round
        """
        # resets the list of tasks for the round
        self.round_tasks = OrderedDict()
        for task_id, task in self.tasks.items():
            resource_id = task.mapping
            resource_load = self.resources[resource_id].load
            if resource_load > threshold:
                # task is in an overloaded resource
                self.round_tasks[task_id] = task

    def set_bundle_of_tasks_once_for_round(self):
        """
        Sets the tasks for the round as a set of bundles of tasks.
        These bundles are set only once on the first round.
        """
        # Creates bundles of tasks only once
        if len(self.round_tasks) is 0:
            self.round_tasks = TaskBundle.create_simple_bundles(
                self.tasks, self.experiment_info.bundle_load_limit,
                self.num_resources())

    def set_underloaded_resources_for_round(self):
        """
        Sets the resources for the round as the resources that are
        currently underloaded.
        """
        # resets the list of resources for the round
        self.round_targets = OrderedDict()
        for resource_id, resource in self.resources.items():
            # Adds the resource to the list if it is underloaded
            load = resource.load
            if resource.load < self.avg_load:
                self.round_targets[resource_id] = Resource(load)

    def prepare_round(self):
        """
        Prepares the context for a scheduling round.

        A simple round uses a simple copy of the tasks and resources for
        scheduling decisions.
        """
        # Sets all tasks for the round
        self.round_tasks = self.tasks
        # Copies all resources for the round
        # a copy is needed because the algorithms use old information
        self.round_resources = copy.deepcopy(self.resources)
        self.round_targets = self.round_resources
        # Handle round logging
        self.log_round()

    def prepare_round_with_limited_tasks(self, threshold):
        """
        Prepares the context for a scheduling round using a threshold to
        define the resources that can have tasks migrated.

        Parameters
        ----------
        threshold : float
            Load that a resource must surpass to add tasks to the round
        """
        # Sets the tasks for the round as the tasks from resources
        # with a load superior to a threshold
        self.set_tasks_with_threshold(threshold)
        # Copies all resources for the round
        # a copy is needed because the algorithms use old information
        self.round_resources = copy.deepcopy(self.resources)
        self.round_targets = self.round_resources
        # Handle round logging
        self.log_round()

    def prepare_round_with_limited_resources(self, threshold):
        """
        Prepares the context for a scheduling round using a threshold to
        define the resources that can have tasks migrated, and adding only
        underloaded resources as candidates to receive tasks.

        Parameters
        ----------
        threshold : float
            Load that a resource must surpass to add tasks to the round
        """
        # Sets the tasks for the round as the tasks from resources
        # with a load superior to a threshold
        self.set_tasks_with_threshold(threshold)
        # Sets the resources for the round as the resources that are
        # currently underloaded.
        self.round_resources = copy.deepcopy(self.resources)
        self.set_underloaded_resources_for_round()
        # Handle round logging
        self.log_round()

    def prepare_round_bundled(self):
        """
        Prepares the context for a scheduling round.

        A simple round uses a simple copy of the tasks and resources for
        scheduling decisions.
        """
        # Sets the bundle of tasks to be used during the round
        self.set_bundle_of_tasks_once_for_round()
        # Copies all resources for the round
        # a copy is needed because the algorithms use old information
        self.round_resources = copy.deepcopy(self.resources)
        self.round_targets = self.round_resources
        # Handle round logging
        self.log_round()

    def get_random_resource(self):
        """
        Returns a random resource from the resources available for the round

        Returns
        -------
        resource_id, Resource object
        """
        resources = self.round_targets
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
        current_load = self.round_resources[current_id].load
        candidate_load = self.round_resources[candidate_id].load

        if self.logging is True:
            self.logger.register_load_check(current_id, current_load,
                                            candidate_id, candidate_load)

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

    def log_start(self):
        """
        Starts headers in the logger.

        Notes
        -----
        Information that is provided:
        - number of tasks, number of resources, RNG seed, scheduler's name
        - maximum resource load
        - number of resources that are overloaded, underloaded, or near the
        average load
        """
        if self.logging is True:
            # first batch of information
            self.logger.register_start(self.experiment_info)

    def log_finish(self):
        """
        Stops logging an experiment.
        """
        if self.logging is True:
            experiment_status = self.gather_status()
            self.logger.register_resource_status(experiment_status)
            # stops logging
            self.logger.register_end()
