# Example on how to read a CSV report
from sched.scheduler import Scheduler

scheduler = Scheduler.from_csv("csv_input_example/description.txt")
scheduler.stats()
scheduler.report()
