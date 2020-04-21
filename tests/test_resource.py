#!/usr/bin/env python3

import unittest
import sys
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.resources import Resource   # noqa


class ResourceTest(unittest.TestCase):
    def test_int_load(self):
        resource = Resource(10)
        self.assertEqual(resource.load, 10)

    def test_float_load(self):
        resource = Resource(10.0)
        self.assertEqual(resource.load, 10.0)

    def test_update_load(self):
        resource = Resource(10)
        resource.load = 100
        self.assertEqual(resource.load, 100)


if __name__ == '__main__':
    unittest.main()
