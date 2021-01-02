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
but modified a bit to add a level of nesting in the "connects" field.

"""


connections = [
    {"_id": 0, "airport": "JFK", "connects": [
        {"to": "BOS", "distance": 200}, {"to": "ORD", "distance": 800}]},
    {"_id": 1, "airport": "BOS", "connects": [
        {"to": "JFK", "distance": 200}, {"to": "PWM", "distance": 2000}]},
    {"_id": 2, "airport": "ORD", "connects": [{"to": "JFK", "distance": 800}]},
    {"_id": 3, "airport": "PWM", "connects": [
        {"to": "BOS", "distance": 2000}, {"to": "LHR", "distance": 6000}]},
    {"_id": 4, "airport": "LHR", "connects": [{"to": "PWM", "distance": 6000}]},
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
            'connectFromField': "connects.to",
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
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'BOS'},
                                             {'distance': 800,
                                                'to': 'ORD'}]),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'JFK'},
                                             {'distance': 2000,
                                                'to': 'PWM'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects',
                                            [{'distance': 800,
                                              'to': 'JFK'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects',
                                            [{'distance': 2000,
                                              'to': 'BOS'},
                                             {'distance': 6000,
                                                'to': 'LHR'}]),
                                           ('numConnections', 2)])],
             'name': 'Dev',
             'nearestAirport': 'JFK'},
            {'_id': 2,
             'destinations': [OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'BOS'},
                                             {'distance': 800,
                                                'to': 'ORD'}]),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'JFK'},
                                             {'distance': 2000,
                                                'to': 'PWM'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects',
                                            [{'distance': 800,
                                              'to': 'JFK'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects',
                                            [{'distance': 2000,
                                              'to': 'BOS'},
                                             {'distance': 6000,
                                                'to': 'LHR'}]),
                                           ('numConnections', 2)])],
             'name': 'Eliot',
             'nearestAirport': 'JFK'},
            {'_id': 3,
             'destinations': [OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'JFK'},
                                             {'distance': 2000,
                                                'to': 'PWM'}]),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'BOS'},
                                             {'distance': 800,
                                                'to': 'ORD'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects',
                                            [{'distance': 2000,
                                              'to': 'BOS'},
                                             {'distance': 6000,
                                                'to': 'LHR'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects',
                                            [{'distance': 800,
                                              'to': 'JFK'}]),
                                           ('numConnections', 2)]),
                              OrderedDict([('_id', 4),
                                           ('airport', 'LHR'),
                                           ('connects',
                                            [{'distance': 6000,
                                              'to': 'PWM'}]),
                                           ('numConnections', 2)])],
             'name': 'Jeff',
             'nearestAirport': 'BOS'}]


class CollectionAPITest(TestCase):

    def setUp(self):
        super(CollectionAPITest, self).setUp()
        self.client = mongomock.MongoClient()
        self.db = self.client['somedb']

    def test_graph_nested_array(self):
        self.db.a.insert_many(connections)
        self.db.b.insert_many(people)
        actual = self.db.b.aggregate(query)
        actual = list(actual)
        """If this test fails it could be because we are comparing dictionaries and lists
        We need normalize the objects before comparing.
        """
        self.assertEqual(expected, actual)
