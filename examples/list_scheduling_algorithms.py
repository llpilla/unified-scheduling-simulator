from simulator.context import Context
from simulator.scheduler import ListScheduler, LPT

my_input = Context.from_csv('../input_examples/tiny_example.csv')

listscheduler = ListScheduler()
lpt = LPT()

# Runs one schedulers after the other
listscheduler.schedule(my_input)
lpt.schedule(my_input)
