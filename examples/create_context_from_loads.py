from simulator.context import Context
from simulator.scheduler import RoundRobin
from simulator.tasks import LoadGenerator

loads = LoadGenerator.uniform(scale=2, size=10, low=1, high=10, rng_seed=4,
                              as_int=True)
context = Context.from_loads(task_loads=loads, num_resources=4, rng_seed=4,
                             name='from_uniform')
scheduler = RoundRobin(screen_verbosity=1, logging_verbosity=0,
                       file_prefix='rr')
scheduler.schedule(context)
context.to_csv('new_input.csv')
