from simulator.context import Context
from simulator.scheduler import RoundRobin

my_input = Context.from_csv('../input_examples/tiny_example.csv')
scheduler = RoundRobin(screen_verbosity=2,
                       logging_verbosity=2,
                       file_prefix='rr')

scheduler.schedule(my_input)
my_input.to_csv('rr_mapping.csv')
