"""
Heap module. Contain min and max heaps for scheduling algorithms.
"""

import heapq


class MinHeap:
    """
    Min heap interface for heapq.
    Used for scheduling.
    """
    def __init__(self):
        self.heap = []

    def push(self, load, identifier):
        heapq.heappush(self.heap, (load, identifier))

    def pop(self):
        return heapq.heappop(self.heap)

    def __len__(self):
        return len(self.heap)


class MaxHeap(MinHeap):
    """
    Max heap interface for heapq.
    Used for scheduling.
    """

    def push(self, load, identifier):
        heapq.heappush(self.heap, (-load, identifier))

    def pop(self):
        load, identifier = heapq.heappop(self.heap)
        return -load, identifier
