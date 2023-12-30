"""A database encapsulating collections of near-Earth objects and their close approaches.

A `NEODatabase` holds an interconnected data set of NEOs and close approaches.
It provides methods to fetch an NEO by primary designation or by name, as well
as a method to query the set of close approaches that match a collection of
user-specified criteria.

Under normal circumstances, the main module creates one NEODatabase from the
data on NEOs and close approaches extracted by `extract.load_neos` and
`extract.load_approaches`.

You'll edit this file in Tasks 2 and 3.
"""
import operator


class NEODatabase:
    """A database of near-Earth objects and their close approaches.

    A `NEODatabase` contains a collection of NEOs and a collection of close
    approaches. It additionally maintains a few auxiliary data structures to
    help fetch NEOs by primary designation or by name and to help speed up
    querying for close approaches that match criteria.
    """
    def __init__(self, neos, approaches):
        """Create a new `NEODatabase`.

        As a precondition, this constructor assumes that the collections of NEOs
        and close approaches haven't yet been linked - that is, the
        `.approaches` attribute of each `NearEarthObject` resolves to an empty
        collection, and the `.neo` attribute of each `CloseApproach` is None.

        However, each `CloseApproach` has an attribute (`._designation`) that
        matches the `.designation` attribute of the corresponding NEO. This
        constructor modifies the supplied NEOs and close approaches to link them
        together - after it's done, the `.approaches` attribute of each NEO has
        a collection of that NEO's close approaches, and the `.neo` attribute of
        each close approach references the appropriate NEO.

        :param neos: A collection of `NearEarthObject`s.
        :param approaches: A collection of `CloseApproach`es.
        """
        self._neos = neos
        self._approaches = approaches

        self.name_to_neo = {neo.name: neo for neo in self._neos}
        self.pdes_to_neo = {neo.designation: neo for neo in self._neos}
        self.approach_to_des = {approach: approach._designation for approach in self._approaches}

        for approach in self._approaches:
            neo = self.get_neo_by_designation(approach._designation)
            approach.neo = neo
            neo.approaches.append(approach)

        # TODO: What additional auxiliary data structures will be useful?

        # TODO: Link together the NEOs and their close approaches.

    def get_neo_by_designation(self, designation):
        """Find and return an NEO by its primary designation.

        If no match is found, return `None` instead.

        Each NEO in the data set has a unique primary designation, as a string.

        The matching is exact - check for spelling and capitalization if no
        match is found.

        :param designation: The primary designation of the NEO to search for.
        :return: The `NearEarthObject` with the desired primary designation, or `None`.
        """
        # TODO: Fetch an NEO by its primary designation.
        return self.pdes_to_neo.get(designation)

    def get_neo_by_name(self, name):
        """Find and return an NEO by its name.

        If no match is found, return `None` instead.

        Not every NEO in the data set has a name. No NEOs are associated with
        the empty string nor with the `None` singleton.

        The matching is exact - check for spelling and capitalization if no
        match is found.

        :param name: The name, as a string, of the NEO to search for.
        :return: The `NearEarthObject` with the desired name, or `None`.
        """
        # TODO: Fetch an NEO by its name.
        return self.name_to_neo.get(name)

    def query(self, filters=()):
        """Query close approaches to generate those that match a collection of filters.

        This generates a stream of `CloseApproach` objects that match all of the
        provided filters.

        If no arguments are provided, generate all known close approaches.

        The `CloseApproach` objects are generated in internal order, which isn't
        guaranteed to be sorted meaningfully, although is often sorted by time.

        :param filters: A collection of filters capturing user-specified criteria.
        :return: A stream of matching `CloseApproach` objects.
        """
        # TODO: Generate `CloseApproach` objects that match all of the filters.
        if not filters:
            for approach in self._approaches:
                yield approach

        else:
            for approach in self._approaches:
                if all(f(approach) for f in filters):
                    yield approach

    def _check_filters(self, approach, filters):
        """To facilitate querying by returning a Bool after comparing each CloseApproach to the user-defined filters."""
        eq = operator.eq
        le = operator.le
        ge = operator.ge

        mapping = {
            'des': (approach._designation, eq),
            'date': (approach.time.date(), eq),
            'start_date': (approach.time.date(), ge),
            'end_date': (approach.time.date(), le),
            'distance_min': (float(approach.distance), ge),
            'distance_max': (float(approach.distance), le),
            'velocity_max': (float(approach.velocity), le),
            'velocity_min': (float(approach.velocity), ge),
            'name': (approach.neo.name, eq),
            'diameter_min': (float(approach.neo.diameter), ge),
            'diameter_max': (float(approach.neo.diameter), le),
            'hazardous': (approach.neo.hazardous, eq)
        }

        for key in filters.keys():
            op = mapping.get(key)
            if not op:
                raise KeyError('No option to search for %s' % key)

            filter_expression = filters.get(key)
            if filter_expression is None:
                yield True
            else:
                yield op[1](op[0], filter_expression)
