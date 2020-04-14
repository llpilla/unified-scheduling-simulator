# This code has only been tested with Python 3.
"""
Context module. Contains the representations of tasks and resources for
scheduling.

Each task has an identifier, a load, and a mapping.
Each resource has an identifier and a load.
The whole context contains a list of tasks, a list of resources, and some
scheduling statistics.
"""

import csv                      # for handling csv files
from collections import OrderedDict


class Task:
    """
    Representation of a task for scheduling.

    Attributes
    ----------
    load : int
        Load of the task
    mapping : int
        Mapping of the task to a resource
    """

    def __init__(self, load=0, mapping=0):
        """Creates a task with a load and a mapping"""
        self.load = load
        self.mapping = mapping


class Resource:
    """
    Representation of a resource for scheduling.

    Attributes
    ----------
    load : int
        Load of the resource
    """

    def __init__(self, load=0):
        """Creates a resource with an id and a load"""
        self.load = load


class Statistics:
    """
    Statistics of the scheduling context.
    Containts information related to tasks, resources, and migrations.

    Attributes
    ----------
    migrations : int
        Number of task migrations during a scheduling round
    num_tasks : int
        Number of tasks
    num_resources : int
        Number of resources
    rng_seed : int
        Random number generator seed
    algorithm : string
        Name of scheduling algorithm
    report : bool
        True if scheduling information should be reported during execution
    """

    def __init__(self, num_tasks=0, num_resources=0,
                 rng_seed=0, algorithm='none', report=False):
        """Creates scheduling statistics"""
        self.migrations = 0
        self.num_tasks = num_tasks
        self.num_resources = num_resources
        self.rng_seed = rng_seed
        self.algorithm = algorithm
        self.report = report


class Context:
    """
    Scheduling context. Contains tasks, resources, and statistics.

    Attributes
    ----------
    tasks : OrderedDict of Task
        List of all tasks
    resources : OrderedDict of Resource
        List of all resources
    stats : Statistics object
        Statistics of the scheduling context
    """
    def __init__(self):
        """Creates an empty scheduling context"""
        self.tasks = OrderedDict()
        self.resources = OrderedDict()
        self.stats = Statistics()

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
        num_tasks = self.stats.num_tasks
        # Checks the number of tasks
        if len(tasks) != num_tasks:
            return False
        # Checks the identifiers and loads of tasks
        for identifier, task in tasks.items():
            if identifier >= num_tasks:
                return False
            if task.load < 0:
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
                # Then we write this information in an Statistics object
                context.stats = Statistics(
                    num_tasks=int(first_info["tasks"]),
                    num_resources=int(first_info["resources"]),
                    rng_seed=int(first_info["rng_seed"]),
                    algorithm=first_info["algorithm"],
                    )

                # After that, we read the other lines of the CSV file
                reader = csv.DictReader(csvfile)
                for line in reader:
                    # Each line generates a task
                    context.tasks[int(line['task_id'])] = Task(
                        load=int(line['task_load']),
                        mapping=int(line['task_mapping']),
                        )

                # Finally, we generate the list of resources
                # and update them based on tasks mapped to them
                for identifier in range(context.stats.num_resources):
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
                stats = self.stats
                comment = "# tasks:" + str(stats.num_tasks) + \
                          " resources:" + str(stats.num_resources) + \
                          " rng_seed:" + str(stats.rng_seed) + \
                          " algorithm:" + stats.algorithm + "\n"
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
        # Prints information about the new mapping
        if self.stats.report is True:
            print(("- Task {task} (load {load})" +
                   " migrating from {old} to {new}.")
                  .format(task=str(task_id),
                          load=str(task.load),
                          old=str(current_resource),
                          new=str(new_resource),
                          ))
