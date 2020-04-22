#!/usr/bin/env python3

import unittest
import sys
from collections import OrderedDict
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.tasks import Task, TaskBundle   # noqa


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


if __name__ == '__main__':
    unittest.main()
