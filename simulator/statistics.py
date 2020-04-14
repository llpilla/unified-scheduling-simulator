"""
Statistics module. Computes load statistics about tasks or machines.

A statistics object holds information about tasks or machines, such as
their mean or median load. This information is useful for reporting
and plotting scheduling information.
"""

import numpy                # for statistics methods
import scipy.stats          # for statistics methods

class Statistics:
    """
    Load statistics

    Attributes
    ----------
    mean : float
        Mean load
    median : float
        Median load
    max : float
        Maximum load
    min : float
        Minimum load
    ratio : float
        Ratio between maximum and minimum loads
    std : float
        Load standard deviation
    skew : float
        Load skewness
    kurt : float
        Load kurtosis
    imbalance : float
        Load imbalance (ratio between maximum and mean loads)
    """
    def __init__(self, loads):
        """Computes statistics based on the loads"""
        self.mean = numpy.mean(loads)           # mean load
        self.median = numpy.median(loads)       # median load
        self.max = max(loads)                   # max load
        self.min = min(loads)                   # min load
        # We can only compute the ratio if the minimum load is not zero
        try:
            self.ratio = self.max/self.min
        except ZeroDivisionError:
            print("Load ratio set to 1 due to a minimum load equal to 0")
            self.ratio = 1.0
        self.std = numpy.std(loads)             # std load
        self.skew = scipy.stats.skew(loads)     # skewness
        self.kurt = scipy.stats.kurtosis(loads) # kurtosis
        # We can only compute the imbalance if the mean load is not zero
        try:
            self.imbalance = self.max/self.mean - 1.0
        except ZeroDivisionError:
            print("Load imbalance set to 0 due to a mean load equal to 0")
            self.imbalance = 0.0
