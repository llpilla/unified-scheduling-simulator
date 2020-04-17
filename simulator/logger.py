"""
Logging module. Stores and prints information related to a scheduling
simulation.
"""

import shutil
from io import StringIO


class Logger:
    """
    Logging class. Stores buffers with text and prints text to the standard
    output or files

    Attributes
    ----------
    screen_verbosity : int
        Level of information to be reported during execution
    logging_verbosity : int
        Level of information to be stored during execution
    log_buffer : StringIO
        Text buffer containing scheduling actions
    stats_buffer : StringIO
        Text buffer containing scheduling results
    file_prefix : string
        Prefix for the name of output files (log and stats)
    num_migrations : int
        Number of tasks migrated during execution
    num_round : int
        Number of the round
    num_load_checks : int
        Number of times the load of a resource was checked
    """

    def __init__(self, screen_verbosity, logging_verbosity, file_prefix):
        self.screen_verbosity = screen_verbosity
        self.logging_verbosity = logging_verbosity
        self.log_buffer = StringIO()
        self.stats_buffer = StringIO()
        self.file_prefix = file_prefix
        self.num_migrations = 0
        self.num_round = 0
        self.num_load_checks = 0

    def __handle_string(self, output, min_verbosity=1):
        """
        Adds string to the stardand output or log if the minimum verbosy
        level is respected.

        Parameters
        ----------
        output : string
            String to print and/or log
        min_verbosity : int, optional (stardard = 1)
            Minimum verbosity level to print and/or log the string

        Notes
        -----
        Verbosity levels:
        0 - nothing is reported
        1 - basic statistics are reported (number of tasks & resources, loads)
        2 - every action is reported
        """
        if self.screen_verbosity >= min_verbosity:
            print(output)
        if self.logging_verbosity >= min_verbosity:
            self.log_buffer.write(output + '\n')

    def register_start(self, info):
        """
        Registers the start of an experiment

        Parameters
        ----------
        info : ExperimentInformation object
            Basic information about the experiment
        """
        # Text for the standard output and log
        output = (f'\n-- Start of experiment --\n' +
                  f'- Running scheduler {info.algorithm}' +
                  f' for {info.num_tasks} tasks and' +
                  f' {info.num_resources} resources.\n' +
                  f'- [RNG seed ({info.rng_seed});' +
                  f' Bundle load limit ({info.bundle_load_limit});' +
                  f' Epsilon ({info.epsilon})]')
        self.__handle_string(output, 1)

        # Text for the experiment statistics
        self.stats_buffer.write(str(info) + '\n')
        header = ('round,maxload,overloaded,underloaded,avgloaded,' +
                  'migration_count,load_checks\n')
        self.stats_buffer.write(header)

    def register_end(self):
        """
        Registers the end of an experiment. Flushes buffers to files.
        """
        # Flushes log buffer
        self.__handle_string('-- End of experiment --', 1)
        if self.logging_verbosity > 0:
            with open(self.file_prefix + '_log.txt', 'w') as logfile:
                self.log_buffer.seek(0)
                shutil.copyfileobj(self.log_buffer, logfile)
        # Flushes stats buffer
        with open(self.file_prefix + '_stats.csv', 'w') as statsfile:
            self.stats_buffer.seek(0)
            shutil.copyfileobj(self.stats_buffer, statsfile)

    def register_resource_status(self, status):
        """
        Registers the status of resources on a given round

        Parameters
        ----------
        status: ExperimentStatus object
            Information about the current status of the simulation
        """
        output = (f'[{self.num_round}] -- Status:\n' +
                  f'- maximum load is {status.max_resource_load}\n' +
                  f'- number of overloaded, underloaded and average loaded' +
                  f' resources are {status.num_overloaded},' +
                  f' {status.num_underloaded}, and' +
                  f' {status.num_averageloaded}\n' +
                  f'- number of migrations is {self.num_migrations}\n' +
                  f'- number of load checks is {self.num_load_checks}')
        self.__handle_string(output, 1)

        status_line = (f'{self.num_round},' +
                       str(status) +
                       f',{self.num_migrations}' +
                       f',{self.num_load_checks}\n')
        self.stats_buffer.write(status_line)

    def register_migration(self, task_id, task_load, from_res, to_res):
        """
        Registers the migration of a task from one resource to another.

        Parameters
        ----------
        task_id : int
            Identifier of the task
        task_load : int or float
            Load of the task
        from_res : int
            Identifier of the source resource
        to_res : int
            Identifier of the destination resource
        """
        self.num_migrations += 1
        output = (f'[{self.num_round}] - Task {task_id}' +
                  f' (load {task_load})' +
                  f' migrating from {from_res} to {to_res}.')
        self.__handle_string(output, 2)

    def register_new_round(self):
        """Registers the start of a new scheduling round"""
        self.num_migrations = 0
        self.num_round += 1
        self.num_load_checks = 0

        output = f'-- Start of round {self.num_round} --'
        self.__handle_string(output, 1)

    def register_convergence(self, convergence, max_load, objective_load):
        """
        Registers if the scheduler has converged to an acceptable solution.

        Parameters
        ----------
        convergence : bool
            True if the convergence has been achieved
        max_load : int or float
            Maximum resource load
        objective_load : int or float
            Maximum resource load objective
        """
        output = (f'[{self.num_round}] -- Convergence check: {max_load} <=' +
                  f' {objective_load} : {convergence}')
        self.__handle_string(output, 1)

    def register_load_check(self, from_res, from_load, to_res, to_load):
        """
        Registers the communication of load information between resources.

        Parameters
        ----------
        from_res : int
            Identifier of the resource who asked for load information
        from_load : int or float
            Load of the requesting resource
        to_res : int
            Identifier of the resource who is answering
        to_load : int or float
            Load of the answering resource
        """
        self.num_load_checks += 1
        output = (f'[{self.num_round}] - Resource {from_res}' +
                  f' (load {from_load})' +
                  f' requesting information from resource {to_res}' +
                  f' (load {to_load})')
        self.__handle_string(output, 2)
