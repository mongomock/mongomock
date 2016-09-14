import json
import os

from mongomock.helpers import embedded_item_getter
from mongomock.helpers import hashdict
from mongomock.helpers import parse_dbase_from_uri
from mongomock.helpers import print_deprecation_warning
from unittest import TestCase


class HashdictTest(TestCase):
    def test__hashdict(self):
        """Make sure hashdict can be used as a key for a dict"""
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


class TestAllUriScenarios(TestCase):
    pass


_URI_SPEC_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    os.path.join('connection_string', 'test'))


def create_uri_spec_tests():
    """Use json specifications in `_TEST_PATH` to generate uri spec tests.

    This is a simplified version from the PyMongo "test/test_uri_spec.py". It
    is modified to disregard warnings and only check that valid uri's are valid
    with the correct database.
    """
    def create_uri_spec_test(scenario_def):
        def run_scenario(self):
            self.assertTrue(scenario_def['tests'], "tests cannot be empty")
            for test in scenario_def['tests']:
                dsc = test['description']

                error = False

                try:
                    dbase = parse_dbase_from_uri(test['uri'])
                except Exception as e:
                    print(e)
                    error = True

                self.assertEqual(not error, test['valid'],
                                 "Test failure '%s'" % dsc)

                # Compare auth options.
                auth = test['auth']
                if auth is not None:
                    expected_dbase = auth.pop('db')  # db == database
                    # Special case for PyMongo's collection parsing
                    if expected_dbase and '.' in expected_dbase:
                        expected_dbase, _ = expected_dbase.split('.', 1)
                    self.assertEqual(expected_dbase, dbase,
                                     "Expected %s but got %s"
                                     % (expected_dbase, dbase))
        return run_scenario

    for dirpath, _, filenames in os.walk(_URI_SPEC_TEST_PATH):
        dirname = os.path.split(dirpath)
        dirname = os.path.split(dirname[-2])[-1] + '_' + dirname[-1]

        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = json.load(scenario_stream)
            # Construct test from scenario.
            new_test = create_uri_spec_test(scenario_def)
            test_name = 'test_%s_%s' % (
                dirname, os.path.splitext(filename)[0])
            new_test.__name__ = test_name
            setattr(TestAllUriScenarios, new_test.__name__, new_test)


create_uri_spec_tests()


class TestHelpers(TestCase):
    def test01_embedded_item_getter(self):
        assert embedded_item_getter("a.b", "c", "a")({"a": {"b": 1}, "c": 5}) == (1, 5, {"b": 1})
