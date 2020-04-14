from simulator.context import Context
import simulator.scheduler as sched

context = Context.from_csv("tiny_example.csv")
print("Round Robin")
round_robin = sched.RoundRobinScheduler(True)
round_robin.schedule(context)

print("Compact")
compact = sched.CompactScheduler(True)
compact.schedule(context)

print("List Scheduler")
list_scheduler = sched.ListScheduler(True)
list_scheduler.schedule(context)

print("LPT Scheduler")
lpt_scheduler = sched.LPTScheduler(True)
lpt_scheduler.schedule(context)
