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


class HeapFactory:
    """
    Collection of methods to create heaps for scheduling.

    Methods
    -------
    create_unloaded_heap(size, heap_type='min')
        Creates a heap where all members are started with zero.
    created_loaded_heap(items, heap_type='min')
        Creates a heap using the load of the items (resources or tasks).
    """

    @staticmethod
    def start_heap(heap_type):
        """
        Starts a MinHeap or MaxHeap object.

         Parameters
        ----------
        heap_type : string ('min' or 'max')
            Type of heap (min or max)

        Returns
        -------
        MinHeap or MaxHeap object
        """
        # Starts heap object
        if heap_type is 'min':
            heap = MinHeap()
        elif heap_type is 'max':
            heap = MaxHeap()
        else:
            print("Incorrect type of heap: "+heap_type)
            print("Returing a min heap")
            heap = MinHeap()
        return heap

    @staticmethod
    def create_unloaded_heap(size, heap_type='min'):
        """
        Creates a heap where all members are started with zero.

        Parameters
        ----------
        size : int
            Number of items in the heap.
        heap_type : string, optional (='min')
            Type of heap (min or max)

        Returns
        -------
        MinHeap or MaxHeap object
        """
        # Starts heap object
        heap = HeapFactory.start_heap(heap_type)
        for identifier in range(size):
            # Each item starts with an empty load
            heap.push(0, identifier)
        return heap

    @staticmethod
    def create_loaded_heap(items, heap_type='min'):
        """
        Creates a heap using the load of the items (resources or tasks).

        Parameters
        ----------
        items : OrderedDict of Resources or Tasks
            Resources or Tasks to add to the heap.
        heap_type : string, optional (='min')
            Type of heap (min or max)

        Returns
        -------
        MinHeap or MaxHeap object
        """
        # Starts heap object
        heap = HeapFactory.start_heap(heap_type)
        for identifier, item in items.items():
            # Each item starts with an empty load
            heap.push(item.load, identifier)
        return heap
