import collections
import copy
from datetime import datetime, tzinfo, timedelta
from distutils import version  # pylint: disable=no-name-in-module
import platform
import random
import re
import six
from six import assertCountEqual, text_type
import sys
import time
from unittest import TestCase, skipIf
import uuid
import warnings

import mongomock

try:
    from unittest import mock
    _HAVE_MOCK = True
except ImportError:
    try:
        import mock
        _HAVE_MOCK = True
    except ImportError:
        _HAVE_MOCK = False

try:
    from bson import codec_options
    from bson.errors import InvalidDocument
    from bson import tz_util, ObjectId, Regex, decimal128, Timestamp, DBRef
    import pymongo
    from pymongo.collation import Collation
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import ReadPreference
    from pymongo import ReturnDocument
    from pymongo.write_concern import WriteConcern

    _HAVE_PYMONGO = True
except ImportError:
    from mongomock.collection import ReturnDocument
    from mongomock import ObjectId
    from mongomock.read_concern import ReadConcern
    from mongomock.write_concern import WriteConcern
    from tests.utils import DBRef

    _HAVE_PYMONGO = False


warnings.simplefilter('ignore', DeprecationWarning)
IS_PYPY = platform.python_implementation() != 'CPython'


class UTCPlus2(tzinfo):
    def fromutc(self, dt):
        return dt + self.utcoffset(dt)

    def tzname(self, dt):
        return '<dummy UTC+2>'

    def utcoffset(self, dt):
        return timedelta(hours=2)

    def dst(self, dt):
        return timedelta()

"""
Example taken from https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/

"""
connections = [
    { "_id" : 0, "airport" : "JFK", "connects" : [ "BOS", "ORD" ] },
    { "_id" : 1, "airport" : "BOS", "connects" : [ "JFK", "PWM" ] },
    { "_id" : 2, "airport" : "ORD", "connects" : [ "JFK" ] },
    { "_id" : 3, "airport" : "PWM", "connects" : [ "BOS", "LHR" ] },
    { "_id" : 4, "airport" : "LHR", "connects" : [ "PWM" ] },
]

people = [
    { "_id" : 1, "name" : "Dev", "nearestAirport" : "JFK" },
    { "_id" : 2, "name" : "Eliot", "nearestAirport" : "JFK" },
    { "_id" : 3, "name" : "Jeff", "nearestAirport" : "BOS" },
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

expected = [   {   '_id': 1,
        'destinations': [   OrderedDict([   ('_id', 0),
                                            ('airport', 'JFK'),
                                            ('connects', ['BOS', 'ORD']),
                                            ('numConnections', 0)]),
                            OrderedDict([   ('_id', 1),
                                            ('airport', 'BOS'),
                                            ('connects', ['JFK', 'PWM']),
                                            ('numConnections', 1)]),
                            OrderedDict([   ('_id', 2),
                                            ('airport', 'ORD'),
                                            ('connects', ['JFK']),
                                            ('numConnections', 1)]),
                            OrderedDict([   ('_id', 3),
                                            ('airport', 'PWM'),
                                            ('connects', ['BOS', 'LHR']),
                                            ('numConnections', 2)])],
        'name': 'Dev',
        'nearestAirport': 'JFK'},
    {   '_id': 2,
        'destinations': [   OrderedDict([   ('_id', 0),
                                            ('airport', 'JFK'),
                                            ('connects', ['BOS', 'ORD']),
                                            ('numConnections', 0)]),
                            OrderedDict([   ('_id', 1),
                                            ('airport', 'BOS'),
                                            ('connects', ['JFK', 'PWM']),
                                            ('numConnections', 1)]),
                            OrderedDict([   ('_id', 2),
                                            ('airport', 'ORD'),
                                            ('connects', ['JFK']),
                                            ('numConnections', 1)]),
                            OrderedDict([   ('_id', 3),
                                            ('airport', 'PWM'),
                                            ('connects', ['BOS', 'LHR']),
                                            ('numConnections', 2)])],
        'name': 'Eliot',
        'nearestAirport': 'JFK'},
    {   '_id': 3,
        'destinations': [   OrderedDict([   ('_id', 1),
                                            ('airport', 'BOS'),
                                            ('connects', ['JFK', 'PWM']),
                                            ('numConnections', 0)]),
                            OrderedDict([   ('_id', 0),
                                            ('airport', 'JFK'),
                                            ('connects', ['BOS', 'ORD']),
                                            ('numConnections', 1)]),
                            OrderedDict([   ('_id', 3),
                                            ('airport', 'PWM'),
                                            ('connects', ['BOS', 'LHR']),
                                            ('numConnections', 1)]),
                            OrderedDict([   ('_id', 2),
                                            ('airport', 'ORD'),
                                            ('connects', ['JFK']),
                                            ('numConnections', 2)]),
                            OrderedDict([   ('_id', 4),
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

