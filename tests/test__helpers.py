from unittest import TestCase
from mongomock.helpers import hashdict
from mongomock.helpers import print_deprecation_warning


class HashdictTest(TestCase):
    def test__hashdict(self):
        """ Make sure hashdict can be used as a key for a dict """
        h = {}
        _id = hashdict({'a': 1})
        h[_id] = 'foo'
        self.assertEqual(h[_id], 'foo')
        _id = hashdict({'a': {'foo': 2}})
        h[_id] = 'foo'
        self.assertEqual(h[_id], 'foo')
        _id = hashdict({'a': {'foo': {'bar': 3}}})
        h[_id] = 'foo'
        self.assertEqual(h[_id], 'foo')
        _id = hashdict({hashdict({'a': '3'}): {'foo': 2}})
        h[_id] = 'foo'
        self.assertEqual(h[_id], 'foo')

class TestDeprecationWarning(TestCase):
    def test__deprecation_warning(self):
        # ensure this doesn't throw an exception
        print_deprecation_warning('aaa', 'bbb')
