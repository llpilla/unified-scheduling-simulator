"""
Scheduler module. Contains scheduling algorithms.

Scheduling algorithms receive a context and reschedule tasks.
"""

#from simulator.context import Context

#import os                           # for creating directories
#import datetime                     # for date (for files)

class Scheduler:    # pylint: disable=old-style-class
    """
    Base scheduling algorithm class.

    Attributes
    ----------
    name : string
        Name of the scheduling algorithm
    report : bool
        True if scheduling information should be reported during execution
    rng_seed : int
        Random number generator seed
    """

    def __init__(self, name="empty-scheduler", report=False, rng_seed=0):
        """Creates a scheduler with its name, verbosity, and RNG seed"""
        self.name = name
        self.report = report
        self.rng_seed = rng_seed

    def register_on_context(self, context):
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
        Applies the scheduling algorithm over a scheduling context

        Parameters
        ----------
        context : Context object
            Scheduling context to schedule
        """
        self.register_on_context(context)
