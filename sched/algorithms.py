"""Scheduling algorithms module. New algorithms are added here."""
import heapq

def roundrobin(tasks, pes):
    """Simple load-unaware round robin algorithm"""
    numtasks = tasks.num
    numpes = pes.num
    mapping = [-1] * numtasks
    for task in range(numtasks):
        mapping[task] = task % numpes
    return mapping

def listscheduling(tasks, pes):
    """Simple load-aware list scheduling algorithm"""
    minheap = []
    # Constructs the min heap in order
    for pe in range(pes.num):
        heapq.heappush(minheap, (0, pe)) # 0 load, pe number

    # Gets the mapping of the tasks in list order
    mapping = [-1] * tasks.num
    for task in range(tasks.num):
        # Gets the current PE with the minimum load and its load
        peload, pe = heapq.heappop(minheap)
        mapping[task] = pe # sets the mapping
        peload += tasks.loads[task] # adds the load of the task to the PE
        heapq.heappush(minheap, (peload, pe))
    return mapping





