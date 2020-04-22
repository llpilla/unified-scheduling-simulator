from simulator.context import Context
import simulator.scheduler as scheds

context = Context.from_csv("csv_input_example/tiny_example.csv")
round_robin = scheds.RoundRobinScheduler(2,2,'__rr')
round_robin.schedule(context)

context = Context.from_csv("csv_input_example/tiny_example.csv")
compact = scheds.CompactScheduler(2,2,'__comp')
compact.schedule(context)

context = Context.from_csv("csv_input_example/tiny_example.csv")
list_scheduler = scheds.ListScheduler(2,2,'__list')
list_scheduler.schedule(context)

context = Context.from_csv("csv_input_example/tiny_example.csv")
lpt_scheduler = scheds.LPTScheduler(2,2,'__lpt')
lpt_scheduler.schedule(context)
