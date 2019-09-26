"""Input module. Contains the representation of data for scheduling."""
import csv                      # for handling csv files
import numpy.random as nr       # for distributions
import numpy                    # for statistics
import scipy.stats as stats     # for statistics

# Class to describe tasks
class Tasks:
    """List of tasks to provide to a scheduler"""
    def __init__(self, num, loads=None):
        """Creates a list of tasks with or without their loads"""
        self.name = "Task"
        self.color = '#05bb06'
        self.num = num
        if isinstance(loads, numpy.ndarray):
            self.loads = numpy.ndarray.tolist(loads)
        else:
            if loads != None:
                self.loads = loads

    @staticmethod
    def from_loads(loads):
        """Creates a list of tasks from their list of loads"""
        return Tasks(len(loads), loads)

    @staticmethod
    def uniform(low=1.0, high=10.0, size=10.0, seed=None):
        """Creates a list of tasks with a uniform distribution of loads"""
        nr.seed(seed)
        return Tasks.from_loads(nr.uniform(low, high, size))

    @staticmethod
    def lognormal(mean=0.0, sigma=1.0, size=10.0, scale=1.0, seed=None):
        """Creates a list of tasks with a lognormal distribution of loads"""
        nr.seed(seed)
        return Tasks.from_loads(scale * nr.lognormal(mean, sigma, size))

    @staticmethod
    def normal(loc=10.0, scale=1.0, size=10.0, seed=None):
        """Creates a list of tasks with a normal distribution of loads"""
        nr.seed(seed)
        return Tasks.from_loads(nr.normal(loc, scale, size))

    @staticmethod
    def range(minvalue=1, maxvalue=10, step=1):
        """Creates a list of tasks with a range of loads"""
        return Tasks.from_loads(range(minvalue, maxvalue, step))

    @staticmethod
    def from_csv(filename="tasks.csv"):
        """Imports task loads from a CSV format"""
        try:
            with open(filename, 'r') as csvfile:
                reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)
                loads = list(reader)[0]
            return Tasks.from_loads(loads)
        except IOError:
            print("Error: could not read file "+filename)

    def to_csv(self, filename="tasks.csv"):
        """Exports task loads to a CSV format"""
        try:
            with open(filename, 'w') as csvfile:
                writer = csv.writer(csvfile)
                try:
                    writer.writerow(self.loads)
                except AttributeError:
                    writer.writerow([0] * self.num)
        except IOError:
            print("Error: could not write file "+filename)


    def stats(self):
        """Computes statistics from the tasks (related to their loads)"""
        try:
            self.mean_load = numpy.mean(self.loads)
            self.median_load = numpy.median(self.loads)
            self.max_load = max(self.loads)
            self.min_load = min(self.loads)
            try:
                self.ratio_load = self.max_load/self.min_load
            except ZeroDivisionError:
                self.ratio_load = 1.0
            self.std_load = numpy.std(self.loads)
        except AttributeError:
            pass

    def report(self, mapping=None):
        """Reports statistics in standard output"""
        print("--- Tasks' report ---")
        try:
            print("Load per task:")
            print(self.loads)
            print("Load arithmetic mean:", self.mean_load)
            print("Load median:", self.median_load)
            print("Load max:", self.max_load)
            print("Load min:", self.min_load)
            print("Load ratio between max and min:", self.ratio_load)
            print("Load standard deviation:", self.std_load)
        except AttributeError:
            print("No information about tasks' loads")
        if mapping != None:
            print("Mapping of tasks to PEs:")
            print(mapping)
        print("--- end of tasks' report ---")

# Class to describe PEs
class PEs:
    """Information about the PEs to provide to a scheduler"""
    def __init__(self, num):
        self.num = num
        self.name = "PE"
        self.color = '#0506bb'

    @staticmethod
    def from_csv(filename="pes.csv"):
        """Imports PE loads from a CSV format"""
        try:
            with open(filename, 'r') as csvfile:
                reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)
                loads = list(reader)[0]
            pes = PEs(len(loads))
            pes.loads = loads
            return pes
        except IOError:
            print("Error: could not read file " + filename)

    def to_csv(self, filename="pes.csv"):
        """Exports PE loads to a CSV format"""
        try:
            with open(filename, 'w') as csvfile:
                writer = csv.writer(csvfile)
                try:
                    writer.writerow(self.loads)
                except AttributeError:
                    writer.writerow([0]*self.num)
        except IOError:
            print("Error: could not write file " + filename)

    def update(self, mapping, tasks):
        """Updates information about PEs based on information from the tasks"""
        self.tasks = []
        for pe in range(self.num):
            # for each PE, sets all tasks that are mapped to it
            self.tasks.append([i for i, x in enumerate(mapping) if x == pe])
        try:
            if tasks.loads[0]: # checking if we have task load information
                self.loads = [0] * self.num
                for pe in range(self.num):
                    for t in self.tasks[pe]:
                        # for each task in said PE, add its load to the load of the PE
                        self.loads[pe] = self.loads[pe] + tasks.loads[t]
        except AttributeError:
            pass

    def stats(self):
        """Computes statistics from the PEs (related to their loads and mapped tasks)"""
        try:
            self.mean_load = numpy.mean(self.loads)
            self.median_load = numpy.median(self.loads)
            self.max_load = max(self.loads)
            self.min_load = min(self.loads)
            try:
                self.ratio_load = self.max_load/self.min_load
            except ZeroDivisionError:
                self.ratio_load = 1.0
            self.std_load = numpy.std(self.loads)
            if float(self.mean_load) != 0.0:
                self.imbalance = self.max_load/self.mean_load - 1
            else:
                self.imbalance = 0.0
            self.skew_load = stats.skew(self.loads)
            self.kurt_load = stats.kurtosis(self.loads)
            self.tasks_per_pe = [0]*self.num
            for i in range(self.num):
                self.tasks_per_pe[i] = len(self.tasks[i])
        except AttributeError:
            pass

    def report(self):
        """Reports statistics in standard output"""
        print("--- PEs' report ---")
        try:
            print("Load per PE:")
            print(self.loads)
            print("Load arithmetic mean:", self.mean_load)
            print("Load median:", self.median_load)
            print("Load max:", self.max_load)
            print("Load min:", self.min_load)
            print("Load ratio between max and min:", self.ratio_load)
            print("Load standard deviation:", self.std_load)
            print("Load skewness:", self.skew_load)
            print("Load kurtosis:", self.kurt_load)
            print("Load imbalance:", self.imbalance)
        except AttributeError:
            print("No information about tasks' loads")
        try:
            print("Tasks mapped to each PE:")
            print(self.tasks)
            print("#Tasks per PE:")
            print(self.tasks_per_pe)
        except AttributeError:
            print("No information about mapped tasks")
        print("--- end of PEs' report ---")
