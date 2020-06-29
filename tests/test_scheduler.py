#!/usr/bin/env python3

import unittest
import sys
import os
import random
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


class SelfishALMethodsTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.SelfishAL(screen_verbosity=0,
                                      logging_verbosity=0,
                                      rng_seed=5)
        self.context = DistributedContext.from_csv(
            'test_inputs/01234_input.csv')

    def test_round_with_threshold(self):
        self.scheduler.sender_subset = 'sup'
        self.scheduler.round_with_threshold(self.context)

        tasks = self.context.round_tasks
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[1].load, 5.0)
        self.assertEqual(tasks[3].load, 3.0)

        self.scheduler.sender_subset = 'avg'
        self.scheduler.round_with_threshold(self.context)

        tasks = self.context.round_tasks
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[1].load, 5.0)
        self.assertEqual(tasks[3].load, 3.0)

        self.scheduler.sender_subset = 'inf'
        self.scheduler.round_with_threshold(self.context)

        tasks = self.context.round_tasks
        self.assertEqual(len(tasks), 5)
        self.assertEqual(tasks[0].load, 1.0)
        self.assertEqual(tasks[4].load, 2.0)

    def test_migration_check_with_threshold(self):
        self.context.update_mapping(0, 0)  # Resource 0 is now avg-loaded
        self.scheduler.prepare_round(self.context)

        self.scheduler.receiver_subset = 'sup'
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 0)
        self.assertTrue(check)
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 2)
        self.assertTrue(check)

        self.scheduler.receiver_subset = 'avg'
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 0)
        self.assertTrue(check)
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 2)
        self.assertTrue(check)

        self.scheduler.receiver_subset = 'inf'
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 0)
        self.assertFalse(check)
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 2)
        self.assertTrue(check)

        self.context.update_mapping(4, 0)  # Resource 0 is now avg-loaded
        self.scheduler.prepare_round(self.context)

        self.scheduler.receiver_subset = 'sup'
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 0)
        self.assertTrue(check)

        self.scheduler.receiver_subset = 'avg'
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 0)
        self.assertFalse(check)

        self.scheduler.receiver_subset = 'inf'
        random.seed(14)
        check = self.scheduler.check_migration(self.context, 1, 1, 0)
        self.assertFalse(check)


class SelfishALTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.SelfishAL(screen_verbosity=0,
                                      logging_verbosity=0,
                                      rng_seed=5)
        self.context = DistributedContext.from_csv(
            'test_inputs/bundle_input.csv')

    def test_selfishAL(self):
        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_s_s_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_sup_sup(self):
        self.scheduler.sender_subset = 'sup'
        self.scheduler.receiver_subset = 'sup'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_s_s_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_sup_avg(self):
        self.scheduler.sender_subset = 'sup'
        self.scheduler.receiver_subset = 'avg'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_s_a_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_sup_inf(self):
        self.scheduler.sender_subset = 'sup'
        self.scheduler.receiver_subset = 'inf'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_s_i_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_avg_sup(self):
        self.scheduler.sender_subset = 'avg'
        self.scheduler.receiver_subset = 'sup'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_a_s_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_avg_avg(self):
        self.scheduler.sender_subset = 'avg'
        self.scheduler.receiver_subset = 'avg'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_a_a_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_avg_inf(self):
        self.scheduler.sender_subset = 'avg'
        self.scheduler.receiver_subset = 'inf'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_a_i_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_inf_sup(self):
        self.scheduler.sender_subset = 'inf'
        self.scheduler.receiver_subset = 'sup'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_i_s_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_inf_avg(self):
        self.scheduler.sender_subset = 'inf'
        self.scheduler.receiver_subset = 'avg'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_i_a_5.csv'
            with open(expected_name, 'r') as expected:
                self.assertEqual(result.read(), expected.read())
        os.remove('experiment.csv')

    def test_inf_inf(self):
        self.scheduler.sender_subset = 'inf'
        self.scheduler.receiver_subset = 'inf'

        self.scheduler.schedule(self.context)
        self.context.to_csv('experiment.csv')
        with open('experiment.csv') as result:
            expected_name = 'test_inputs/expected_selfish_al_i_i_5.csv'
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
