# Example of plotting experiments
from sched.inputs import Tasks, PEs
from sched.scheduler import Scheduler

tasks = Tasks.normal(40, 5, 50, 16) # generate tasks from a normal distribution
pes = PEs(10)

roundrobin = Scheduler(tasks, pes, "roundrobin")
roundrobin.work()
roundrobin.stats()
roundrobin.report()
roundrobin.plot_all()

listscheduler = Scheduler(tasks, pes, "listscheduling")
listscheduler.work()
listscheduler.stats()
listscheduler.report()
listscheduler.plot_all()
