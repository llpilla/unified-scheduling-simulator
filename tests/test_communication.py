#!/usr/bin/env python3

import unittest
import sys
# Add the parent directory to the path so we can import
# code from our simulator
sys.path.append('../')

from simulator.communication import Vertex, Graph   # noqa


class VertexTest(unittest.TestCase):
    def setUp(self):
        self.vertex = Vertex(10)
        self.vertex.add_communication(2)
        self.vertex.add_communication(3, 20, 30)
        self.vertex.add_communication(neigh_id=4, volume=40, msgs=50)

    def test_add_communication(self):
        vertex = self.vertex
        self.assertEqual(vertex.task_id, 10)

        self.assertEqual(vertex.volume[2], 1)
        self.assertEqual(vertex.volume[3], 20)
        self.assertEqual(vertex.volume[4], 40)

        self.assertEqual(vertex.msgs[2], 1)
        self.assertEqual(vertex.msgs[3], 30)
        self.assertEqual(vertex.msgs[4], 50)

    def test_remove_communication(self):
        vertex = self.vertex

        vertex.remove_communication(2)
        self.assertNotIn(2, vertex.volume)
        self.assertNotIn(2, vertex.msgs)
        self.assertIn(3, vertex.volume)
        self.assertIn(3, vertex.msgs)

        vertex.remove_communication(3)
        self.assertNotIn(3, vertex.volume)
        self.assertNotIn(3, vertex.msgs)

    def test_get_neighbors(self):
        vertex = self.vertex

        neighbors = vertex.get_neighbors()
        self.assertEqual(len(neighbors), 3)
        self.assertEqual(neighbors[0], 2)
        self.assertEqual(neighbors[1], 3)
        self.assertEqual(neighbors[2], 4)

    def test_get_communication(self):
        vertex = self.vertex

        vol, msgs = vertex.get_communication(3)
        self.assertEqual(vol, 20)
        self.assertEqual(msgs, 30)
        vol, msgs = vertex.get_communication(17)
        self.assertEqual(vol, 0)
        self.assertEqual(msgs, 0)


class GraphTest(unittest.TestCase):
    def setUp(self):
        self.directed_graph = Graph(3)
        self.undirected_graph = Graph(3, False)

        self.directed_graph.add_communication(0, 0)
        self.undirected_graph.add_communication(0, 0)

        self.directed_graph.add_communication(0, 1, 10, 20)
        self.undirected_graph.add_communication(0, 1, 10, 20)

        self.directed_graph.add_communication(0, 2, 20, 30)
        self.undirected_graph.add_communication(0, 2, 20, 30)

        self.directed_graph.add_communication(1, 0, volume=40, msgs=50)
        self.undirected_graph.add_communication(1, 0, volume=40, msgs=50)

        self.directed_graph.add_communication(source_id=1, dest_id=2)
        self.undirected_graph.add_communication(source_id=1, dest_id=2)

    def test_add_communication_directed(self):
        vertices = self.directed_graph.vertices
        directed = self.directed_graph.directed

        self.assertTrue(directed)
        self.assertEqual(len(vertices), 3)
        self.assertEqual(vertices[0].volume[0], 1)
        self.assertEqual(vertices[0].msgs[0], 1)
        self.assertEqual(vertices[0].volume[1], 10)
        self.assertEqual(vertices[0].msgs[1], 20)
        self.assertEqual(vertices[0].volume[2], 20)
        self.assertEqual(vertices[0].msgs[2], 30)
        self.assertEqual(vertices[1].volume[0], 40)
        self.assertEqual(vertices[1].msgs[0], 50)
        self.assertEqual(vertices[1].volume[2], 1)
        self.assertEqual(vertices[1].msgs[2], 1)

    def test_add_communication_undirected(self):
        vertices = self.undirected_graph.vertices
        directed = self.undirected_graph.directed

        self.assertFalse(directed)
        self.assertEqual(len(vertices), 3)
        self.assertEqual(vertices[0].volume[0], 1)
        self.assertEqual(vertices[0].msgs[0], 1)
        self.assertEqual(vertices[0].volume[1], 50)
        self.assertEqual(vertices[0].msgs[1], 70)
        self.assertEqual(vertices[0].volume[2], 20)
        self.assertEqual(vertices[0].msgs[2], 30)
        self.assertEqual(vertices[1].volume[0], 50)
        self.assertEqual(vertices[1].msgs[0], 70)
        self.assertEqual(vertices[1].volume[2], 1)
        self.assertEqual(vertices[1].msgs[2], 1)
        self.assertEqual(vertices[2].volume[0], 20)
        self.assertEqual(vertices[2].msgs[0], 30)
        self.assertEqual(vertices[2].volume[1], 1)
        self.assertEqual(vertices[2].msgs[1], 1)

    def test_remove_communication(self):
        self.directed_graph.remove_communication(0, 0)
        self.directed_graph.remove_communication(0, 1)
        vertices = self.directed_graph.vertices
        self.assertNotIn(0, vertices[0].volume)
        self.assertNotIn(1, vertices[0].volume)
        self.assertIn(0, vertices[1].volume)

        self.undirected_graph.remove_communication(0, 0)
        self.undirected_graph.remove_communication(0, 1)
        vertices = self.undirected_graph.vertices
        self.assertNotIn(0, vertices[0].volume)
        self.assertNotIn(1, vertices[0].volume)
        self.assertNotIn(0, vertices[1].volume)

    def test_get_communication(self):
        vol, msgs = self.directed_graph.get_communication(0, 1)
        self.assertEqual(vol, 10)
        self.assertEqual(msgs, 20)
        vol, msgs = self.directed_graph.get_communication(2, 0)
        self.assertEqual(vol, 0)
        self.assertEqual(msgs, 0)

    def test_get_vertex(self):
        vertex = self.directed_graph.get_vertex(0)
        self.assertEqual(vertex.task_id, 0)
        self.assertEqual(len(vertex.volume), 3)
        self.assertEqual(vertex.volume[0], 1)


if __name__ == '__main__':
    unittest.main()
