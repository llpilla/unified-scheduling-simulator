"""
Communication module. Contains the representation of the communication
between two tasks (an edge in a communication graph).

Each communication has a source, a destination, a number of messages,
and a number of bytes.
"""

from collections import OrderedDict


class Vertex:
    """
    Representation of a task in the communication graph.

    Attributes
    ----------
    task_id : int
        Task identifier
    volume : OrderedDict
        Dictionary of volume of bytes communicated with other tasks
    msgs : OrderedDict
        Dictionary of number of messages communicated with other tasks
    """

    def __init__(self, task_id):
        """Creates a vertex."""
        self.task_id = task_id
        self.volume = OrderedDict()
        self.msgs = OrderedDict()

    def add_communication(self, neigh_id, volume=1, msgs=1):
        """
        Adds or updates a communication edge.

        Parameters
        ----------
        neigh_id : int
            Identifier of the neighbor task
        volume : int (default = 1)
            Number of bytes communicated
        msgs : int (default = 1)
            Number of messages communicated
        """
        # If we have this neighbor, we update it
        if neigh_id in self.volume:
            self.volume[neigh_id] += volume
            self.msgs[neigh_id] += msgs
        else:  # If we do not have it, we add it
            self.volume[neigh_id] = volume
            self.msgs[neigh_id] = msgs

    def remove_communication(self, neigh_id):
        """
        Removes a communication edge

        Parameters
        ----------
        neigh_id : int
            Identifier of the neighbor task
        """
        if neigh_id in self.volume:
            self.volume.pop(neigh_id)
            self.msgs.pop(neigh_id)

    def get_neighbors(self):
        """
        Returns a list of neighbors.

        Returns
        -------
        list of int
            List of neighbor identifiers
        """
        return list(self.volume.keys())

    def get_communication(self, neigh_id):
        """
        Returns the bytes and msgs exchanged with a neighbor.
        Returns zero if no communication was present.

        Parameters
        ----------
        neigh_id : int
            Identifier of the neighbor task

        Returns
        -------
        int, int
            Number of bytes and number of messages in the communication edge.
        """
        if neigh_id in self.volume:
            return self.volume[neigh_id], self.msgs[neigh_id]
        else:
            return 0, 0


class Graph:
    """
    Representation of the communication graph.

    Attributes
    ----------
    vertices : list of Vertex
        Vertices of the graph
    directed : bool (default: True)
        True if the graph is directed
    """

    def __init__(self, num_tasks, directed=True):
        """
        Creates a communication graph.

        Parameters
        ----------
        num_tasks : int
            Number of tasks in the graph.
        directed : bool (default: True)
            True if the graph is directed
        """
        self.directed = directed
        self.vertices = []
        for i in range(num_tasks):
            self.vertices.append(Vertex(i))

    def add_communication(self, source_id, dest_id, volume=1, msgs=1):
        """
        Adds or updates a communication edge.

        Parameters
        ----------
        source_id : int
            Identifier of the source task
        dest_id : int
            Identifier of the destination task
        volume : int (default = 1)
            Number of bytes communicated
        msgs : int (default = 1)
            Number of messages communicated
        """
        self.vertices[source_id].add_communication(dest_id, volume, msgs)
        if (self.directed is False) and (source_id != dest_id):
            self.vertices[dest_id].add_communication(source_id, volume, msgs)

    def remove_communication(self, source_id, dest_id):
        """
        Removes a communication edge

        Parameters
        ----------
        source_id : int
            Identifier of the source task
        dest_id : int
            Identifier of the destination task
        """
        self.vertices[source_id].remove_communication(dest_id)
        if (self.directed is False) and (source_id != dest_id):
            self.vertices[dest_id].remove_communication(source_id)

    def get_communication(self, source_id, dest_id):
        """
        Returns the bytes and msgs exchanged of a communication edge.
        Returns zero if no communication was present.

        Parameters
        ----------
        source_id : int
            Identifier of the source task
        dest_id : int
            Identifier of the destination task

        Returns
        -------
        int, int
            Number of bytes and number of messages in the communication edge.
        """
        return self.vertices[source_id].get_communication(dest_id)

    def get_vertex(self, task_id):
        """
        Returns a vertex from the communication graph.

        Returns
        -------
        Vertex object
            Task from the communication graph
        """
        return self.vertices[task_id]

    def check_consistency(self):
        """
        Verifies that the communication graph is consistent.

        Returns
        -------
        bool
            True if the graph is consistent

        Notes
        -----
        A consistent communication graph only has edges to
        existing tasks, and the values for bytes and volume are
        positive.
        """
        num_tasks = len(self.vertices)

        for vertex in self.vertices:
            volume = vertex.volume
            msgs = vertex.msgs
            # First check: neighbors are valid tasks
            if not(all(task_id < num_tasks for task_id in volume.keys()) and
                   all(task_id >= 0 for task_id in volume.keys())):
                return False
            # Second check: numbers of bytes and messages are positive
            if not(all(vol > 0 for vol in volume.values()) and
                   all(msg > 0 for msg in msgs.values())):
                return False
        # If no inconsistencies were found, we are good
        return True
