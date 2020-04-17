"""
Resources module. Contains the representations of resources for scheduling.

Each resource has a load.
"""


class Resource:
    """
    Representation of a resource for scheduling.

    Attributes
    ----------
    load : int or float
        Load of the resource
    """

    def __init__(self, load=0):
        """Creates a resource with an id and a load."""
        self.load = load
