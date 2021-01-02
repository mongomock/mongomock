import collections
import copy
from datetime import datetime, tzinfo, timedelta
import random
from unittest import TestCase, skipIf
import warnings

import mongomock

warnings.simplefilter('ignore', DeprecationWarning)


"""
Example taken from https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/

"""
connections = [
    {"_id": 0, "airport": "JFK", "connects": ["BOS", "ORD"]},
    {"_id": 1, "airport": "BOS", "connects": ["JFK", "PWM"]},
    {"_id": 2, "airport": "ORD", "connects": ["JFK"]},
    {"_id": 3, "airport": "PWM", "connects": ["BOS", "LHR"]},
    {"_id": 4, "airport": "LHR", "connects": ["PWM"]},
]

people = [
    {"_id": 1, "name": "Dev", "nearestAirport": "JFK"},
    {"_id": 2, "name": "Eliot", "nearestAirport": "JFK"},
    {"_id": 3, "name": "Jeff", "nearestAirport": "BOS"},
]

query = [
    {
        '$graphLookup': {
            'from': "a",
            'startWith': "$nearestAirport",
            'connectFromField': "connects",
            'connectToField': "airport",
            'maxDepth': 2,
            'depthField': "numConnections",
            'as': "destinations"
        }
    }
]


"""
The expected output is a pretty-pretty printed form of actual output. It's
manually verified to be correct, and matching the answer on mongodb website,
mentioned above.

Using OrderedDict just because the "pretty-printed" actual output shows it.

"""

from collections import OrderedDict

expected = [{'_id': 1,
             'destinations': [OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects', ['BOS', 'ORD']),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects', ['JFK', 'PWM']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects', ['JFK']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects', ['BOS', 'LHR']),
                                           ('numConnections', 2)])],
             'name': 'Dev',
             'nearestAirport': 'JFK'},
            {'_id': 2,
             'destinations': [OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects', ['BOS', 'ORD']),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects', ['JFK', 'PWM']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects', ['JFK']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects', ['BOS', 'LHR']),
                                           ('numConnections', 2)])],
             'name': 'Eliot',
             'nearestAirport': 'JFK'},
            {'_id': 3,
             'destinations': [OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects', ['JFK', 'PWM']),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects', ['BOS', 'ORD']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects', ['BOS', 'LHR']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects', ['JFK']),
                                           ('numConnections', 2)]),
                              OrderedDict([('_id', 4),
                                           ('airport', 'LHR'),
                                           ('connects', ['PWM']),
                                           ('numConnections', 2)])],
             'name': 'Jeff',
             'nearestAirport': 'BOS'}]


class CollectionAPITest(TestCase):

    def setUp(self):
        super(CollectionAPITest, self).setUp()
        self.client = mongomock.MongoClient()
        self.db = self.client['somedb']

    def test_graph_basic(self):
        self.db.a.insert_many(connections)
        self.db.b.insert_many(people)
        actual = self.db.b.aggregate(query)
        actual = list(actual)
        """If this test fails it could be because we are comparing dictionaries and lists
        We need normalize the objects before comparing.
        """
        self.assertEqual(expected, actual)
