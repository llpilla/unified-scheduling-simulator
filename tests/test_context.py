#!/usr/bin/env python3

import unittest
import sys
import os
import filecmp
from collections import OrderedDict
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.context import ExperimentInformation   # noqa
from simulator.context import ExperimentStatus   # noqa
from simulator.context import Context  # noqa
from simulator.tasks import Task, TaskBundle  # noqa
from simulator.resources import Resource  # noqa


class ExperimentInformationTest(unittest.TestCase):
    def setUp(self):
        info = ExperimentInformation()
        info.num_tasks = 10
        info.num_resources = 5
        info.algorithm = 'None'
        info.rng_seed = 30
        info.bundle_load_limit = 3
        info.epsilon = 1.07
        self.info = info

    def test_attributes(self):
        self.assertEqual(self.info.num_tasks, 10)
        self.assertEqual(self.info.num_resources, 5)
        self.assertEqual(self.info.algorithm, 'None')
        self.assertEqual(self.info.rng_seed, 30)
        self.assertEqual(self.info.bundle_load_limit, 3)
        self.assertEqual(self.info.epsilon, 1.07)

    def test_representation(self):
        r = 'Experiment information (10, 5, None, 30, 3, 1.07)'
        self.assertEqual(self.info.__repr__(), r)

    def test_string(self):
        s = '# tasks:10 resources:5 rng_seed:30 algorithm:None'
        self.assertEqual(str(self.info), s)

    def test_update(self):
        new_info = ExperimentInformation(algorithm='Some',
                                         rng_seed=12,
                                         bundle_load_limit=7,
                                         epsilon=1.09)
        self.info.update_from_scheduler(new_info)
        self.assertEqual(self.info.num_tasks, 10)
        self.assertEqual(self.info.num_resources, 5)
        self.assertEqual(self.info.algorithm, 'Some')
        self.assertEqual(self.info.rng_seed, 12)
        self.assertEqual(self.info.bundle_load_limit, 7)
        self.assertEqual(self.info.epsilon, 1.09)


class ExperimentStatusTest(unittest.TestCase):
    def setUp(self):
        self.status = ExperimentStatus(100.0, 4, 5, 6)

    def test_attributes(self):
        self.assertEqual(self.status.max_resource_load, 100.0)
        self.assertEqual(self.status.num_overloaded, 4)
        self.assertEqual(self.status.num_underloaded, 5)
        self.assertEqual(self.status.num_averageloaded, 6)

    def test_representation(self):
        r = 'Experiment status (100.0, 4, 5, 6)'
        self.assertEqual(self.status.__repr__(), r)

    def test_string(self):
        s = '100.0,4,5,6'
        self.assertEqual(str(self.status), s)


class ContextTest(unittest.TestCase):
    def setUp(self):
        self.context = Context.from_csv('test_inputs/01234_input.csv')

    def test_attributes(self):
        self.assertEqual(len(self.context.tasks), 5)
        self.assertEqual(self.context.tasks[0].load, 1)
        self.assertEqual(len(self.context.resources), 3)
        self.assertEqual(self.context.resources[0].load, 4)

        info = self.context.experiment_info
        self.assertEqual(info.num_tasks, 5)
        self.assertEqual(info.num_resources, 3)
        self.assertEqual(info.algorithm, 'none')
        self.assertEqual(info.rng_seed, 0)
        self.assertEqual(info.bundle_load_limit, 10)
        self.assertEqual(info.epsilon, 1.05)

        self.assertFalse(self.context.logging)
        self.assertEqual(self.context.logger, None)

    def test_basic_functions(self):
        self.assertEqual(self.context.num_tasks(), 5)
        self.assertEqual(self.context.num_resources(), 3)
        self.assertEqual(self.context.avg_resource_load(), 5.0)
        self.assertEqual(self.context.max_resource_load(), 8)

    def test_gather_status(self):
        status = self.context.gather_status()
        s = '8.0,1,2,0'
        self.assertEqual(str(status), s)

    def test_classify_resources(self):
        ov, un, av = self.context.classify_resources()
        self.assertEqual(ov, 1)
        self.assertEqual(un, 2)
        self.assertEqual(av, 0)

    def test_check_consistency(self):
        self.assertTrue(self.context.check_consistency())
        # invalid mapping
        self.context.tasks[0].mapping = 20
        self.assertFalse(self.context.check_consistency())
        self.context.tasks[0].mapping = 2
        self.assertTrue(self.context.check_consistency())
        # invalid load
        self.context.tasks[0].load = -10
        self.assertFalse(self.context.check_consistency())
        self.context.tasks[0].load = 1
        self.assertTrue(self.context.check_consistency())
        # too many tasks
        self.context.tasks[10] = Task()
        self.assertFalse(self.context.check_consistency())
        self.context.tasks.pop(10)
        self.assertTrue(self.context.check_consistency())
        # too many resources
        self.context.resources[10] = Resource
        self.assertFalse(self.context.check_consistency())
        self.context.resources.pop(10)
        self.assertTrue(self.context.check_consistency())
        # invalid load
        self.context.resources[0].load = -10
        self.assertFalse(self.context.check_consistency())
        self.context.resources[0].load = 8
        self.assertTrue(self.context.check_consistency())

    def test_to_csv(self):
        self.context.to_csv()
        diff = filecmp.cmp('scenario.csv', 'test_inputs/01234_input.csv')
        self.assertTrue(diff)
        os.remove('scenario.csv')

    def test_log(self):
        self.context.set_verbosity(0, 0)
        self.assertFalse(self.context.logging)
        self.assertEqual(self.context.logger, None)

        self.context.set_verbosity(0, 1)
        self.assertTrue(self.context.logging)

    def test_update_mapping(self):
        self.context.update_mapping(0, 1)
        self.assertEqual(self.context.tasks[0].mapping, 1)
        self.assertEqual(self.context.resources[1].load, 9.0)
        self.context.update_mapping(0, 1)
        self.assertEqual(self.context.tasks[0].mapping, 1)
        self.assertEqual(self.context.resources[1].load, 9.0)

    def test_update_mapping_bundled(self):
        bundle = TaskBundle()
        bundle.set_mapping(2)
        bundle.add_task(0, self.context.tasks[0])
        bundle.add_task(4, self.context.tasks[4])
        self.context.round_tasks = OrderedDict()
        self.context.round_tasks[0] = bundle
        self.context.update_mapping_bundled(0, 1)
        self.assertEqual(self.context.tasks[0].mapping, 1)
        self.assertEqual(self.context.tasks[4].mapping, 1)
        self.assertEqual(self.context.resources[1].load, 11.0)


if __name__ == '__main__':
    unittest.main()
