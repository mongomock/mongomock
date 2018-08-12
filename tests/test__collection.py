from mongomock.collection import get_value_by_dot, set_value_by_dot
from unittest import TestCase


class CollectionTest(TestCase):

    def test__get_value_by_dot_missing_key(self):
        """Test get_value_by_dot raises KeyError when looking for a missing key"""
        for doc, key in (
                ({}, 'a'),
                ({'a': 1}, 'b'),
                ({'a': 1}, 'a.b'),
                ({'a': {'b': 1}}, 'a.b.c'),
                ({'a': {'b': 1}}, 'a.c'),
                ({'a': [{'b': 1}]}, 'a.b'),
                ({'a': [{'b': 1}]}, 'a.1.b')):
            self.assertRaises(KeyError, get_value_by_dot, doc, key)

    def test__get_value_by_dot_find_key(self):
        """Test get_value_by_dot when key can be found"""
        for doc, key, expected in (
                ({'a': 1}, 'a', 1),
                ({'a': {'b': 1}}, 'a', {'b': 1}),
                ({'a': {'b': 1}}, 'a.b', 1),
                ({'a': [{'b': 1}]}, 'a.0.b', 1)):
            found = get_value_by_dot(doc, key)
            self.assertEqual(found, expected)

    def test__set_value_by_dot(self):
        """Test set_value_by_dot"""
        for doc, key, expected in (
                ({}, 'a', {'a': 42}),
                ({'a': 1}, 'a', {'a': 42}),
                ({'a': {'b': 1}}, 'a', {'a': 42}),
                ({'a': {'b': 1}}, 'a.b', {'a': {'b': 42}}),
                ({'a': [{'b': 1}]}, 'a.0', {'a': [42]}),
                ({'a': [{'b': 1}]}, 'a.0.b', {'a': [{'b': 42}]})):
            ret = set_value_by_dot(doc, key, 42)
            assert ret is doc
            self.assertEqual(ret, expected)

    def test__set_value_by_dot_bad_key(self):
        """Test set_value_by_dot when key has an invalid parent"""
        for doc, key in (
                ({}, 'a.b'),
                ({'a': 1}, 'a.b'),
                ({'a': {'b': 1}}, 'a.b.c'),
                ({'a': [{'b': 1}]}, 'a.1.b'),
                ({'a': [{'b': 1}]}, 'a.1')):
            self.assertRaises(KeyError, set_value_by_dot, doc, key, 42)
