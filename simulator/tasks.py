"""
Tasks module. Contains the representations of tasks and bundles of tasks.

Each task has a load, and a mapping.
Each bundle of tasks has a series of tasks, a load, and a mapping
"""

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


class TaskBundle(Task):
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
    def check_task_loads(tasks, bundle_load_limit):
        """
        Checks if all tasks have loads smaller than the bundle limit.

        Parameters
        ----------
        tasks : OrderedDict of Task objects
            Tasks to bundle
        bundle_load_limit : int or float
            Load limit of bundles

        Returns
        -------
        bool
            True if all tasks have loads smaller than the limit
        """
        for task in tasks.values():
            if task.load > bundle_load_limit:
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
    def create_simple_bundles(tasks, bundle_load_limit, num_resources):
        """
        Bundles tasks based on their mapping and a bundle size (load limit).

        Parameters
        ----------
        tasks : OrderedDict of Task objects
            Tasks to bundle
        bundle_load_limit : int or float
            Load limit of bundles
        num_resources : int
            Number of resources

        Returns
        -------
        OrderedDict of TaskBundle objects
        """
        # Checks if tasks are smaller than the load limit of bundles
        if TaskBundle.check_task_loads(tasks, bundle_load_limit) is True:
            # Creates a list of tasks per resource
            mappings = TaskBundle.create_inverse_mapping(tasks,
                                                         num_resources)
            bundles = OrderedDict()
            bundle_id = 0
            # Creates bundles of tasks in order
            # Tasks are inserted in order while they respect the load limit
            for resource_id in range(num_resources):
                # Creates bundle
                bundle = TaskBundle()
                bundle.set_mapping(resource_id)

                # Inserts tasks mapped to the resource
                for task_id in mappings[resource_id]:
                    task = tasks[task_id]
                    # Checks if the task fits into the current bundle
                    if bundle.load + task.load > bundle_load_limit:
                        # It does not, so we add the current full bundle
                        # to the list and start a new one
                        bundles[bundle_id] = bundle
                        bundle_id += 1
                        bundle = TaskBundle()
                        bundle.set_mapping(resource_id)
                    # Adds task to the bundle
                    bundle.add_task(task_id, task)

                # Checks if the last bundle of the resource has something
                if bundle.is_empty() is False:
                    bundles[bundle_id] = bundle
                    bundle_id += 1

            return bundles
        else:
            print('Could not create bundle of tasks due to their large loads')
            return None
