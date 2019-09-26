# Example of the report functionality
from sched.inputs import Tasks, PEs
from sched.scheduler import Scheduler

tasks = Tasks(30)
pes = PEs(5)
scheduler = Scheduler(tasks,pes,"roundrobin")
scheduler.work()
print("# Calling directly on the Tasks object")
tasks.stats()
tasks.report(scheduler.mapping)
print("# Calling directly on the PEs object")
pes.stats()
pes.report()
print("# Calling from the Scheduler object")
# no need to call scheduler.stats() because we already did it for Tasks and PEs
scheduler.report()
