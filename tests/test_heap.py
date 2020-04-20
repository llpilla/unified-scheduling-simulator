#!/usr/bin/env python3

import unittest
import sys
from collections import OrderedDict
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.heap import HeapFactory   # noqa
from simulator.resources import Resource  # noqa


class MinHeapTest(unittest.TestCase):
    def setUp(self):
        heap = HeapFactory.start_heap('min')
        heap.push(100, 10)
        heap.push(500, 50)
        heap.push(200, 20)
        heap.push(300, 30)
        heap.push(400, 40)
        self.heap = heap

    def test_first_pop(self):
        load, identifier = self.heap.pop()
        self.assertEqual(load, 100)
        self.assertEqual(identifier, 10)
        self.assertEqual(len(self.heap), 4)

    def test_second_pop(self):
        load, identifier = self.heap.pop()
        load, identifier = self.heap.pop()
        self.assertEqual(load, 200)
        self.assertEqual(identifier, 20)
        self.assertEqual(len(self.heap), 3)

    def test_push_and_pop(self):
        self.heap.push(600, 60)
        self.heap.push(50, 5)
        self.assertEqual(len(self.heap), 7)

        load, identifier = self.heap.pop()
        self.assertEqual(load, 50)
        self.assertEqual(identifier, 5)
        self.assertEqual(len(self.heap), 6)

        load, identifier = self.heap.pop()
        self.assertEqual(load, 100)
        self.assertEqual(identifier, 10)
        self.assertEqual(len(self.heap), 5)


class MaxHeapTest(unittest.TestCase):
    def setUp(self):
        heap = HeapFactory.start_heap('max')
        heap.push(100, 10)
        heap.push(500, 50)
        heap.push(200, 20)
        heap.push(300, 30)
        heap.push(400, 40)
        self.heap = heap

    def test_first_pop(self):
        load, identifier = self.heap.pop()
        self.assertEqual(load, 500)
        self.assertEqual(identifier, 50)
        self.assertEqual(len(self.heap), 4)

    def test_second_pop(self):
        load, identifier = self.heap.pop()
        load, identifier = self.heap.pop()
        self.assertEqual(load, 400)
        self.assertEqual(identifier, 40)
        self.assertEqual(len(self.heap), 3)

    def test_push_and_pop(self):
        self.heap.push(600, 60)
        self.heap.push(50, 5)
        self.assertEqual(len(self.heap), 7)

        load, identifier = self.heap.pop()
        self.assertEqual(load, 600)
        self.assertEqual(identifier, 60)
        self.assertEqual(len(self.heap), 6)

        load, identifier = self.heap.pop()
        self.assertEqual(load, 500)
        self.assertEqual(identifier, 50)
        self.assertEqual(len(self.heap), 5)


class CreateLoadedHeapTest(unittest.TestCase):
    def setUp(self):
        res1 = Resource(100)
        res2 = Resource(200)
        res3 = Resource(300)
        pairs = OrderedDict([(1, res1), (3, res3), (2, res2)])
        self.heap = HeapFactory.create_loaded_heap(pairs, 'min')

    def test_pop(self):
        load, identifier = self.heap.pop()
        self.assertEqual(load, 100)
        self.assertEqual(identifier, 1)
        self.assertEqual(len(self.heap), 2)

    def test_push_and_pop(self):
        self.heap.push(10, 5)
        load, identifier = self.heap.pop()
        self.assertEqual(load, 10)
        self.assertEqual(identifier, 5)
        self.assertEqual(len(self.heap), 3)


class CreateUnloadedHeapTest(unittest.TestCase):
    def setUp(self):
        self.min_heap = HeapFactory.create_unloaded_heap(3, 'min')
        self.max_heap = HeapFactory.create_unloaded_heap(3, 'max')

    def test_pop(self):
        load, identifier = self.min_heap.pop()
        self.assertEqual(load, 0)
        self.assertEqual(identifier, 0)
        self.assertEqual(len(self.min_heap), 2)

        load, identifier = self.max_heap.pop()
        self.assertEqual(load, 0)
        self.assertEqual(identifier, 0)
        self.assertEqual(len(self.max_heap), 2)

    def test_push_and_pop(self):
        self.min_heap.push(100, 10)
        load, identifier = self.min_heap.pop()
        self.assertEqual(load, 0)
        self.assertEqual(identifier, 0)
        self.assertEqual(len(self.min_heap), 3)

        self.max_heap.push(100, 10)
        load, identifier = self.max_heap.pop()
        self.assertEqual(load, 100)
        self.assertEqual(identifier, 10)
        self.assertEqual(len(self.max_heap), 3)


if __name__ == '__main__':
    unittest.main()
