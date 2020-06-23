#!/usr/bin/env python3

import unittest
import sys
import os
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.context import Context  # noqa
from simulator.context import DistributedContext  # noqa
import simulator.scheduler as sc  # noqa


class SchedulerTest(unittest.TestCase):
    def test_optional_attributes(self):
        scheduler = sc.Scheduler()
        context = Context()
        scheduler.schedule(context)

        info = scheduler.experiment_info
        self.assertEqual(info.algorithm, 'empty-scheduler')
        self.assertEqual(info.rng_seed, 0)
        self.assertEqual(info.bundle_load_limit, 10)
        self.assertEqual(info.epsilon, 1.05)
        self.assertEqual(scheduler.screen_verbosity, 1)
        self.assertEqual(scheduler.logging_verbosity, 1)
        self.assertEqual(scheduler.file_prefix, 'experiment')

        info = context.experiment_info
        self.assertEqual(info.algorithm, 'empty-scheduler')
        self.assertEqual(info.rng_seed, 0)
        self.assertEqual(info.bundle_load_limit, 10)
        self.assertEqual(info.epsilon, 1.05)

        os.remove('experiment_log.txt')
        os.remove('experiment_stats.csv')

    def test_attributes(self):
        scheduler = sc.Scheduler('test_scheduler', 10, 20, 2.0, 0, 0, 'exp')
        context = Context()
        scheduler.schedule(context)

        info = scheduler.experiment_info
        self.assertEqual(info.algorithm, 'test_scheduler')
        self.assertEqual(info.rng_seed, 10)
        self.assertEqual(info.bundle_load_limit, 20)
        self.assertEqual(info.epsilon, 2.0)
        self.assertEqual(scheduler.screen_verbosity, 0)
        self.assertEqual(scheduler.logging_verbosity, 0)
        self.assertEqual(scheduler.file_prefix, 'exp')

        info = context.experiment_info
        self.assertEqual(info.algorithm, 'test_scheduler')
        self.assertEqual(info.rng_seed, 10)
        self.assertEqual(info.bundle_load_limit, 20)
        self.assertEqual(info.epsilon, 2.0)


class RoundRobinTest(unittest.TestCase):
    def test_scheduler(self):
        scheduler = sc.RoundRobin(0, 0)
        self.assertEqual(scheduler.experiment_info.algorithm, 'RoundRobin')
        context = Context.from_csv('test_inputs/01234_input.csv')
        scheduler.schedule(context)

        tasks = context.tasks
        self.assertEqual(tasks[0].mapping, 0)
        self.assertEqual(tasks[1].mapping, 1)
        self.assertEqual(tasks[2].mapping, 2)
        self.assertEqual(tasks[3].mapping, 0)
        self.assertEqual(tasks[4].mapping, 1)

        resources = context.resources
        self.assertEqual(resources[0].load, 4.0)
        self.assertEqual(resources[1].load, 7.0)
        self.assertEqual(resources[2].load, 4.0)


class RandomTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.Random(
            screen_verbosity=0,
            logging_verbosity=0)
        self.context = Context.from_csv('test_inputs/01234_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_random_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv', 'r') as result:
            expected_name = 'test_inputs/expected_random_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class RandomNormalTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.RandomNormal(
            screen_verbosity=0,
            logging_verbosity=0)
        self.context = Context.from_csv('test_inputs/01234_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_randomnormal_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv', 'r') as result:
            expected_name = 'test_inputs/expected_randomnormal_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class RandomExponentiallTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.RandomExponential(
            screen_verbosity=0,
            logging_verbosity=0)
        self.context = Context.from_csv('test_inputs/01234_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_randomexponential_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv', 'r') as result:
            expected_name = 'test_inputs/expected_randomexponential_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class CompactTest(unittest.TestCase):
    def test_scheduler(self):
        scheduler = sc.Compact(0, 0)
        self.assertEqual(scheduler.experiment_info.algorithm, 'Compact')
        context = Context.from_csv('test_inputs/01234_input.csv')
        scheduler.schedule(context)

        tasks = context.tasks
        self.assertEqual(tasks[0].mapping, 0)
        self.assertEqual(tasks[1].mapping, 0)
        self.assertEqual(tasks[2].mapping, 1)
        self.assertEqual(tasks[3].mapping, 1)
        self.assertEqual(tasks[4].mapping, 2)

        resources = context.resources
        self.assertEqual(resources[0].load, 6.0)
        self.assertEqual(resources[1].load, 7.0)
        self.assertEqual(resources[2].load, 2.0)


class ListScheduler(unittest.TestCase):
    def test_scheduler(self):
        scheduler = sc.ListScheduler(0, 0)
        self.assertEqual(scheduler.experiment_info.algorithm, 'ListScheduler')
        context = Context.from_csv('test_inputs/01234_input.csv')
        scheduler.schedule(context)

        tasks = context.tasks
        self.assertEqual(tasks[0].mapping, 0)
        self.assertEqual(tasks[1].mapping, 1)
        self.assertEqual(tasks[2].mapping, 2)
        self.assertEqual(tasks[3].mapping, 0)
        self.assertEqual(tasks[4].mapping, 0)

        resources = context.resources
        self.assertEqual(resources[0].load, 6.0)
        self.assertEqual(resources[1].load, 5.0)
        self.assertEqual(resources[2].load, 4.0)


class LPTTest(unittest.TestCase):
    def test_scheduler(self):
        scheduler = sc.LPT(0, 0)
        self.assertEqual(scheduler.experiment_info.algorithm, 'LPT')
        context = Context.from_csv('test_inputs/01234_input.csv')
        scheduler.schedule(context)

        tasks = context.tasks
        self.assertEqual(tasks[0].mapping, 1)
        self.assertEqual(tasks[1].mapping, 0)
        self.assertEqual(tasks[2].mapping, 1)
        self.assertEqual(tasks[3].mapping, 2)
        self.assertEqual(tasks[4].mapping, 2)

        resources = context.resources
        self.assertEqual(resources[0].load, 5.0)
        self.assertEqual(resources[1].load, 5.0)
        self.assertEqual(resources[2].load, 5.0)


class DistSchedulerTest(unittest.TestCase):
    def test_optional_attributes(self):
        scheduler = sc.DistScheduler()

        info = scheduler.experiment_info
        self.assertEqual(info.algorithm, 'DistScheduler')
        self.assertEqual(info.rng_seed, 0)
        self.assertEqual(info.bundle_load_limit, 10)
        self.assertEqual(info.epsilon, 1.05)
        self.assertEqual(scheduler.screen_verbosity, 1)
        self.assertEqual(scheduler.logging_verbosity, 1)
        self.assertEqual(scheduler.file_prefix, 'experiment')


class SelfishTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.Selfish(screen_verbosity=0, logging_verbosity=0)
        self.context = DistributedContext.from_csv(
            'test_inputs/bundle_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class SelfishALTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.SelfishAL(screen_verbosity=0,
                                      logging_verbosity=0)
        self.context = DistributedContext.from_csv(
            'test_inputs/bundle_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class SelfishAL_fromavgTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.SelfishAL_fromavg(screen_verbosity=0,
                                              logging_verbosity=0)
        self.context = DistributedContext.from_csv(
            'test_inputs/bundle_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_from_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_from_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class SelfishAL_tounderTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.SelfishAL_tounder(screen_verbosity=0,
                                              logging_verbosity=0)
        self.context = DistributedContext.from_csv(
            'test_inputs/bundle_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_to_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_to_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class SelfishAL_fromavg_tounderTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.SelfishAL_fromavg_tounder(screen_verbosity=0,
                                                      logging_verbosity=0)
        self.context = DistributedContext.from_csv(
            'test_inputs/bundle_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_from_to_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_from_to_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class SelfishOUTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.SelfishOU(screen_verbosity=0,
                                      logging_verbosity=0)
        self.context = DistributedContext.from_csv(
            'test_inputs/bundle_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_ou_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_ou_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


class BundledSelfishTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.BundledSelfish(
            screen_verbosity=0,
            logging_verbosity=0,
            bundle_load_limit=5)
        self.context = DistributedContext.from_csv(
            'test_inputs/bundle_input.csv')

    def test_zero_seed(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_bundledselfish_0.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_ten_seed(self):
        self.scheduler.experiment_info.rng_seed = 10
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv', 'r') as result:
            expected_name = 'test_inputs/expected_bundledselfish_10.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')


if __name__ == '__main__':
    unittest.main()
