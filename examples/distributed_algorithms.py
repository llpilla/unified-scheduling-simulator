from simulator.context import DistributedContext
from simulator.scheduler import Selfish, BundledSelfish

# First experiment: run with the Selfish scheduler
context = DistributedContext.from_csv('../input_examples/bundle_example.csv')
selfish = Selfish(screen_verbosity=0,
                  logging_verbosity=1,
                  file_prefix='selfish',
                  rng_seed=0)
selfish.schedule(context)
context.to_csv('selfish_mapping.csv')

# Second experiment: run with the bundled version of the same algorithm
context = DistributedContext.from_csv('../input_examples/bundle_example.csv')
bundled = BundledSelfish(screen_verbosity=0,
                         logging_verbosity=1,
                         file_prefix='bundled',
                         rng_seed=0,
                         bundle_load_limit=5)
bundled.schedule(context)
context.to_csv('bundled_mapping.csv')
