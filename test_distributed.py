from simulator.context import DistributedContext
import simulator.scheduler as scheds

context = DistributedContext.from_csv("csv_input_example/bundle_example.csv")
selfish = scheds.SelfishScheduler(32, 1.05, 1, 2, '__selfish')
selfish.schedule(context)

context = DistributedContext.from_csv("csv_input_example/bundle_example.csv")
bundled = scheds.BundledSelfishScheduler(32, 5, 1.05, 1, 2, '__bundled')
bundled.schedule(context)
