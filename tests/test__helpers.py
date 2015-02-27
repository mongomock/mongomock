from unittest import TestCase
from mongomock.helpers import hashdict


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
