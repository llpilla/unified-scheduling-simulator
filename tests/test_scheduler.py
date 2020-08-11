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
from simulator.heap import HeapFactory #noqa


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


class RefineTest(unittest.TestCase):
    def setUp(self):
        self.scheduler = sc.Refine(screen_verbosity=0, logging_verbosity=0)
        self.context = Context.from_csv('test_inputs/01234_input.csv')

    def test_scheduler(self):
        self.scheduler.schedule(self.context)

        self.assertEqual(self.scheduler.max_overload, 16)
        self.assertEqual(self.scheduler.min_overload, 15)

        tasks = self.context.tasks
        self.assertEqual(tasks[0].mapping, 2)
        self.assertEqual(tasks[1].mapping, 1)
        self.assertEqual(tasks[2].mapping, 0)
        self.assertEqual(tasks[3].mapping, 2)
        self.assertEqual(tasks[4].mapping, 2)

        resources = self.context.resources
        self.assertEqual(resources[0].load, 4.0)
        self.assertEqual(resources[1].load, 5.0)
        self.assertEqual(resources[2].load, 6.0)

    def test_refine(self):
        self.scheduler.experiment_info.epsilon = 1.4
        self.scheduler.compute_load_thresholds(5.0, 8.0)
        self.scheduler.reset_work_context(self.context)
        success = self.scheduler.refine(1.4)
        self.assertTrue(success)

        tasks = self.scheduler.work_context.tasks
        self.assertEqual(tasks[0].mapping, 2)
        self.assertEqual(tasks[1].mapping, 1)
        self.assertEqual(tasks[2].mapping, 0)
        self.assertEqual(tasks[3].mapping, 2)
        self.assertEqual(tasks[4].mapping, 2)

    def test_refine2(self):
        self.scheduler.experiment_info.epsilon = 1.2
        self.scheduler.compute_load_thresholds(5.0, 8.0)
        self.scheduler.reset_work_context(self.context)
        success = self.scheduler.refine(1.2)
        self.assertFalse(success)

        tasks = self.scheduler.work_context.tasks
        self.assertEqual(tasks[0].mapping, 2)
        self.assertEqual(tasks[1].mapping, 1)
        self.assertEqual(tasks[2].mapping, 0)
        self.assertEqual(tasks[3].mapping, 1)
        self.assertEqual(tasks[4].mapping, 2)

    def test_classify_resources(self):
        self.scheduler.reset_work_context(self.context)
        over = HeapFactory.start_heap('max')
        under = []
        self.scheduler.classify_resources(7.0, 3.5, over, under)

        self.assertEqual(len(over), 1)
        resource_load, resource_id = over.pop()
        self.assertEqual(resource_id, 1)
        self.assertEqual(resource_load, 8.0)

        self.assertEqual(len(under), 1)
        self.assertEqual(under[0], 2)

    def test_organize_tasks_per_resource(self):
        self.scheduler.reset_work_context(self.context)
        tasks_on_res = self.scheduler.organize_tasks_per_resource()

        self.assertEqual(tasks_on_res[0][0], 2)
        self.assertEqual(tasks_on_res[1][0], 1)
        self.assertEqual(tasks_on_res[1][1], 3)
        self.assertEqual(tasks_on_res[2][0], 0)
        self.assertEqual(tasks_on_res[2][1], 4)

    def test_compute_load_thresholds(self):
        self.scheduler.compute_load_thresholds(10.0, 20.0)
        self.assertEqual(self.scheduler.min_overload, 0)
        self.assertEqual(self.scheduler.max_overload, 96)

        self.scheduler.experiment_info.epsilon = 1.1
        self.scheduler.compute_load_thresholds(10.0, 20.0)
        self.assertEqual(self.scheduler.min_overload, 0)
        # One would expect 91, but 2.0 - 1.1 = 0.89999999
        self.assertEqual(self.scheduler.max_overload, 90)

    def test_reset_work_context(self):
        self.scheduler.reset_work_context(self.context)

        tasks = self.scheduler.work_context.tasks
        self.assertEqual(tasks[0].mapping, 2)
        self.assertEqual(tasks[1].mapping, 1)
        self.assertEqual(tasks[2].mapping, 0)
        self.assertEqual(tasks[3].mapping, 1)
        self.assertEqual(tasks[4].mapping, 2)

        resources = self.scheduler.work_context.resources
        self.assertEqual(resources[0].load, 4.0)
        self.assertEqual(resources[1].load, 8.0)
        self.assertEqual(resources[2].load, 3.0)

    def test_copy_solution(self):
        self.scheduler.reset_work_context(self.context)

        self.scheduler.work_context.update_mapping(0, 0)
        self.scheduler.work_context.update_mapping(1, 1)
        self.scheduler.work_context.update_mapping(2, 2)
        self.scheduler.work_context.update_mapping(3, 0)
        self.scheduler.work_context.update_mapping(4, 1)

        self.scheduler.copy_solution(self.context)

        tasks = self.context.tasks
        self.assertEqual(tasks[0].mapping, 0)
        self.assertEqual(tasks[1].mapping, 1)
        self.assertEqual(tasks[2].mapping, 2)
        self.assertEqual(tasks[3].mapping, 0)
        self.assertEqual(tasks[4].mapping, 1)

        resources = self.context.resources
        self.assertEqual(resources[0].load, 4.0)
        self.assertEqual(resources[1].load, 7.0)
        self.assertEqual(resources[2].load, 4.0)


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
