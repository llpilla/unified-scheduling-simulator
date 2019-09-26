# Example on the way to save a mapping to a CSV file
from sched.inputs import Tasks, PEs
from sched.scheduler import Scheduler

tasks = Tasks.range(1, 21, 1)
pes = PEs(5)
rrsched = Scheduler(tasks, pes, "roundrobin")
rrsched.work()
rrsched.stats()
rrsched.report() # remove comment if you want to see information in the standard output
rrsched.to_csv()
