#!/usr/bin/env python3

import unittest
import sys
from collections import OrderedDict
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.tasks import Task, TaskBundle, LoadGenerator   # noqa


class TaskTest(unittest.TestCase):
    def test_int_load(self):
        task = Task(10, 5)
        self.assertEqual(task.load, 10)
        self.assertEqual(task.mapping, 5)

    def test_float_load(self):
        task = Task(10.0, 5)
        self.assertEqual(task.load, 10.0)
        self.assertEqual(task.mapping, 5)

    def test_update_task(self):
        task = Task()
        task.load = 10.0
        task.mapping = 5
        self.assertEqual(task.load, 10.0)
        self.assertEqual(task.mapping, 5)


class TaskBundleTest(unittest.TestCase):
    def test_empty_bundle(self):
        bundle = TaskBundle()
        self.assertEqual(bundle.load, 0)
        self.assertEqual(bundle.mapping, 0)
        self.assertEqual(len(bundle.task_ids), 0)
        self.assertTrue(bundle.is_empty())

    def test_mapping(self):
        bundle = TaskBundle()
        bundle.set_mapping(100)
        self.assertEqual(bundle.mapping, 100)

    def test_add_task(self):
        bundle = TaskBundle()
        task = Task(5.0, 10)
        bundle.add_task(2, task)
        self.assertEqual(bundle.load, 5.0)
        self.assertEqual(len(bundle.task_ids), 1)
        self.assertEqual(bundle.task_ids[0], 2)
        self.assertFalse(bundle.is_empty())

    def test_check_task_loads(self):
        tasks = OrderedDict()
        tasks[1] = Task(10, 2)
        tasks[3] = Task(20, 0)
        tasks[2] = Task(5, 0)
        self.assertTrue(TaskBundle.check_task_loads(tasks, 30))
        self.assertTrue(TaskBundle.check_task_loads(tasks, 20))
        self.assertFalse(TaskBundle.check_task_loads(tasks, 19.9999))
        self.assertFalse(TaskBundle.check_task_loads(tasks, 1))

    def test_create_inverse_mapping(self):
        tasks = OrderedDict()
        tasks[1] = Task(10, 2)
        tasks[3] = Task(20, 0)
        tasks[2] = Task(5, 0)
        mapping = TaskBundle.create_inverse_mapping(tasks, 3)
        self.assertEqual(len(mapping[0]), 2)
        self.assertEqual(len(mapping[1]), 0)
        self.assertEqual(len(mapping[2]), 1)
        self.assertEqual(mapping[0][0], 3)
        self.assertEqual(mapping[0][1], 2)
        self.assertEqual(mapping[2][0], 1)

    def test_create_simple_bundles(self):
        tasks = OrderedDict()
        tasks[1] = Task(10, 2)
        tasks[3] = Task(20, 0)
        tasks[2] = Task(5, 0)
        tasks[0] = Task(5, 0)
        tasks[4] = Task(5, 0)
        bundles = TaskBundle.create_simple_bundles(tasks, 20, 3)

        bundle = bundles[0]
        self.assertEqual(bundle.load, 20)
        self.assertEqual(bundle.mapping, 0)
        self.assertEqual(len(bundle.task_ids), 1)
        self.assertEqual(bundle.task_ids[0], 3)

        bundle = bundles[1]
        self.assertEqual(bundle.load, 15)
        self.assertEqual(bundle.mapping, 0)
        self.assertEqual(len(bundle.task_ids), 3)
        self.assertEqual(bundle.task_ids[0], 2)
        self.assertEqual(bundle.task_ids[1], 0)
        self.assertEqual(bundle.task_ids[2], 4)

        bundle = bundles[2]
        self.assertEqual(bundle.load, 10)
        self.assertEqual(bundle.mapping, 2)
        self.assertEqual(len(bundle.task_ids), 1)
        self.assertEqual(bundle.task_ids[0], 1)


class LoadGeneratorTest(unittest.TestCase):
    def test_range(self):
        loads = LoadGenerator.range(size=10, low=0, high=10)
        self.assertEqual(len(loads), 10)
        expected_loads = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        for i in range(10):
            with self.subTest(i=i):
                self.assertEqual(loads[i], expected_loads[i])

    def test_range_scale(self):
        loads = LoadGenerator.range(scale=2, size=10, low=0, high=5)
        self.assertEqual(len(loads), 10)
        expected_loads = [0, 2, 4, 6, 8, 0, 2, 4, 6, 8]
        for i in range(10):
            with self.subTest(i=i):
                self.assertEqual(loads[i], expected_loads[i])

    def test_range_size(self):
        loads = LoadGenerator.range(size=3, low=0, high=10)
        self.assertEqual(len(loads), 3)
        expected_loads = [0, 1, 2]
        for i in range(3):
            with self.subTest(i=i):
                self.assertEqual(loads[i], expected_loads[i])

    def test_range_step(self):
        loads = LoadGenerator.range(size=10, low=0, high=100, step=3)
        self.assertEqual(len(loads), 10)
        expected_loads = [0, 3, 6, 9, 12, 15, 18, 21, 24, 27]
        for i in range(10):
            with self.subTest(i=i):
                self.assertEqual(loads[i], expected_loads[i])

    def test_uniform(self):
        loads = LoadGenerator.uniform(size=5, rng_seed=10)
        self.assertEqual(len(loads), 5)
        expected_loads = [7.941, 1.186, 6.702, 7.739, 5.486]
        for i in range(5):
            with self.subTest(i=i):
                self.assertAlmostEqual(loads[i], expected_loads[i], places=2)

    def test_uniform_as_int(self):
        loads = LoadGenerator.uniform(scale=2.0, size=5, rng_seed=10,
                                      as_int=True)
        self.assertEqual(len(loads), 5)
        expected_loads = [15, 2, 13, 15, 10]
        for i in range(5):
            with self.subTest(i=i):
                self.assertEqual(loads[i], expected_loads[i])

    def test_lognormal(self):
        loads = LoadGenerator.lognormal(size=5, rng_seed=20)
        self.assertEqual(len(loads), 5)
        expected_loads = [2.420, 1.216, 1.429, 0.096, 0.337]
        for i in range(5):
            with self.subTest(i=i):
                self.assertAlmostEqual(loads[i], expected_loads[i], places=2)

    def test_lognormal_as_int(self):
        loads = LoadGenerator.lognormal(scale=2.0, size=5, rng_seed=20,
                                        as_int=True)
        self.assertEqual(len(loads), 5)
        expected_loads = [4, 2, 2, 0, 0]
        for i in range(5):
            with self.subTest(i=i):
                self.assertEqual(loads[i], expected_loads[i])

    def test_normal(self):
        loads = LoadGenerator.normal(size=5, rng_seed=30)
        self.assertEqual(len(loads), 5)
        expected_loads = [1.00, 3.791, 1.293, 2.734, 2.163]
        for i in range(5):
            with self.subTest(i=i):
                self.assertAlmostEqual(loads[i], expected_loads[i], places=2)

    def test_normal_as_int(self):
        loads = LoadGenerator.normal(scale=2.0, size=5, rng_seed=30,
                                     as_int=True)
        self.assertEqual(len(loads), 5)
        expected_loads = [2, 7, 2, 5, 4]
        for i in range(5):
            with self.subTest(i=i):
                self.assertEqual(loads[i], expected_loads[i])

    def test_exponential(self):
        loads = LoadGenerator.exponential(size=5, rng_seed=40)
        self.assertEqual(len(loads), 5)
        expected_loads = [0.523, 0.056, 1.553, 0.338, 0.598]
        for i in range(5):
            with self.subTest(i=i):
                self.assertAlmostEqual(loads[i], expected_loads[i], places=2)

    def test_exponential_as_int(self):
        loads = LoadGenerator.exponential(scale=2.0, size=5, rng_seed=40,
                                          as_int=True)
        self.assertEqual(len(loads), 5)
        expected_loads = [1, 0, 3, 0, 1]
        for i in range(5):
            with self.subTest(i=i):
                self.assertEqual(loads[i], expected_loads[i])


if __name__ == '__main__':
    unittest.main()
