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
* Following test case taken from
      https://stackoverflow.com/questions/40989763/mongodb-graphlookup

* The ISODate(x) helps to keep the original code intact.
"""
def ISODate(x): return x

documents = [
{ "_id" : 1, "name" : "Dev" },
{ "_id" : 2, "name" : "Eliot", "reportsTo" : { 'name': "Dev", "from": ISODate("2016-01-01T00:00:00.000Z") } },
{ "_id" : 3, "name" : "Ron", "reportsTo" : { 'name': "Eliot", "from": ISODate("2016-01-01T00:00:00.000Z") } },
{ "_id" : 4, "name" : "Andrew", "reportsTo" : { 'name': "Eliot", "from": ISODate("2016-01-01T00:00:00.000Z") } },
{ "_id" : 5, "name" : "Asya", "reportsTo" : { 'name': "Ron", "from": ISODate("2016-01-01T00:00:00.000Z") } },
{ "_id" : 6, "name" : "Dan", "reportsTo" : { 'name': "Andrew", "from": ISODate("2016-01-01T00:00:00.000Z") } },
]


"""
      Query from the original stackoverflow article, except the "startWith" is changed.
      The stackoverflow article has "startWith" as "Elliot", which doesn't appear to be correct.
"""
query = [
    {
        '$graphLookup': {
            'from': "a",
            'startWith': "$name",
            'connectFromField': "reportsTo.name",
            'connectToField': "name",
            'as': "reportingHierarchy"
            }
        }
]

""" 
This here is a cut/paste of the actual output of the system. It was verified manually to ascertain
correctness. Incidentally, it is different from the stacck overlow article. 

"""

expected = [   {   '_id': 1,
        'name': 'Dev',
        'reportingHierarchy': [{'_id': 1, 'name': 'Dev'}]},
    {   '_id': 2,
        'name': 'Eliot',
        'reportingHierarchy': [   {   '_id': 2,
                                      'name': 'Eliot',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Dev'}},
                                  {'_id': 1, 'name': 'Dev'}],
        'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Dev'}},
    {   '_id': 3,
        'name': 'Ron',
        'reportingHierarchy': [   {   '_id': 3,
                                      'name': 'Ron',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Eliot'}},
                                  {   '_id': 2,
                                      'name': 'Eliot',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Dev'}},
                                  {'_id': 1, 'name': 'Dev'}],
        'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Eliot'}},
    {   '_id': 4,
        'name': 'Andrew',
        'reportingHierarchy': [   {   '_id': 4,
                                      'name': 'Andrew',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Eliot'}},
                                  {   '_id': 2,
                                      'name': 'Eliot',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Dev'}},
                                  {'_id': 1, 'name': 'Dev'}],
        'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Eliot'}},
    {   '_id': 5,
        'name': 'Asya',
        'reportingHierarchy': [   {   '_id': 5,
                                      'name': 'Asya',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Ron'}},
                                  {   '_id': 3,
                                      'name': 'Ron',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Eliot'}},
                                  {   '_id': 2,
                                      'name': 'Eliot',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Dev'}},
                                  {'_id': 1, 'name': 'Dev'}],
        'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Ron'}},
    {   '_id': 6,
        'name': 'Dan',
        'reportingHierarchy': [   {   '_id': 6,
                                      'name': 'Dan',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Andrew'}},
                                  {   '_id': 4,
                                      'name': 'Andrew',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Eliot'}},
                                  {   '_id': 2,
                                      'name': 'Eliot',
                                      'reportsTo': {   'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                       'name': 'Dev'}},
                                  {'_id': 1, 'name': 'Dev'}],
        'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Andrew'}}]

class CollectionAPITest(TestCase):

    def setUp(self):
        super(CollectionAPITest, self).setUp()
        self.client = mongomock.MongoClient()
        self.db = self.client['somedb']


    def test_graph_nested_dict(self):
        self.db.a.insert_many(documents)
        actual = self.db.a.aggregate(query)
        actual = list(actual)
        """ if this match fails, it could be because the answer contains a list
         They ought to be normalized before comparison."""
        self.assertEqual(expected, actual)

