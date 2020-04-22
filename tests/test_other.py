#!/usr/bin/env python3

import unittest
import sys
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.heap import HeapFactory   # noqa


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


if __name__ == '__main__':
    unittest.main()
