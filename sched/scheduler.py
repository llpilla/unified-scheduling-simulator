"""Scheduler module: groups tasks, PEs, and scheduling algorithms"""
import os                           # for creating directories
import datetime                     # for date (for files)
import csv                          # for handling csv files
from sched.inputs import Tasks, PEs # for simple data types
import sched.algorithms             # for access to the scheduling algorithms (as functions)
from sched.plotter import Plotter   # for plotting results

# Class to interface schedulers to simplify calls
class Scheduler:
    """Simplified interface to call schedulers. Includes analysis tools"""
    def __init__(self, tasks, pes, algorithm):
        """Creates a scheduling scenario"""
        self.tasks = tasks
        self.pes = pes
        self.algorithm = algorithm
        try: # Defining the scheduling function by its name
            self.schedule = getattr(sched.algorithms, algorithm) # Gets the algorithm by its name
        except AttributeError:
            print("Error: could not find scheduling algorithm "+algorithm)

        date = "date:" + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        name = "sched:" + self.algorithm + "+tasks:" + str(tasks.num) + "+pes:"+str(pes.num)
        self.path = name + "+" + date + "/"

    @staticmethod
    def from_csv(description):
        """Imports scheduling information to CSV format from a description file"""
        try: # read information from the description file
            with open(description, 'r') as descfile:
                lines = descfile.read().splitlines()
                info = dict(i.split('=') for i in lines)
        except IOError:
            print("Error: could not read file " + description)
        try: # create objects based on their csv files
            tasks = Tasks.from_csv(info['tasks_file'])
            pes = PEs.from_csv(info['pes_file'])
            scheduler = Scheduler(tasks, pes, info['algorithm'])
        except NameError:
            pass
        try: # read mapping information from csv file
            with open(info['mapping_file'], 'r') as csvfile:
                reader = csv.reader(csvfile, quoting=csv.QUOTE_NONNUMERIC)
                mapping = list(reader)[0]
            scheduler.mapping = [int(x) for x in mapping]
            pes.update(mapping, tasks)
            return scheduler
        except IOError:
            print("Error: could not read file "+info['mapping_file'])

    def to_csv(self, description="description.txt", filename="mapping.csv"):
        """Exports scheduling information to CSV format"""
        try:
            os.mkdir(self.path)
        except OSError:
            pass
            #print "Error: could not create directory "+self.path
        tasks_file = self.path + "tasks.csv"
        pes_file = self.path + "pes.csv"
        mapping_file = self.path + filename
        description_file = self.path + description
        # export csvs
        self.tasks.to_csv(tasks_file)   # export tasks
        self.pes.to_csv(pes_file)       # export pes
        try:                            # export mapping
            with open(mapping_file, 'w') as csvfile:
                writer = csv.writer(csvfile)
                try:
                    writer.writerow(self.mapping)
                except AttributeError:
                    print("Error: no mapping to export")
        except IOError:
            print("Error: could not write file "+mapping_file)
        # export general iinformation of the test
        try:
            with open(description_file, 'w') as descfile:
                descfile.write("algorithm=" + self.algorithm + "\n")
                descfile.write("tasks_file=" + tasks_file + "\n")
                descfile.write("pes_file=" + pes_file + "\n")
                descfile.write("mapping_file=" + mapping_file + "\n")
        except IOError:
            print("Error: could not write file " + description_file)

    def work(self):
        """Applies the scheduling algorithm"""
        self.mapping = self.schedule(self.tasks, self.pes)             # Calls the scheduling algorithm
        self.pes.update(self.mapping, self.tasks)

    def stats(self):
        """Computes statistics related to the mapping"""
        self.tasks.stats()
        self.pes.stats()

    def report(self):
        """Reports statistics in standard output"""
        self.tasks.report(self.mapping)
        self.pes.report()

    def plot_all(self):
        """Plots all possible results"""
        plotter = Plotter(self.path, self.tasks)
        plotter.histogram()
        plotter.bars()
        plotter.bars(True)
        plotter.bars(True, True)
        plotter.bars(False, True)
        plotter = Plotter(self.path, self.pes)
        plotter.histogram()
        plotter.bars()
        plotter.bars(True)
        plotter.bars(True, True)
        plotter.bars(False, True)
