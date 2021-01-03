"""
          UNIT TEST FOR $graphLookup WITH CONNECT FROM FIELD

The test cases are defined in the files:

- fixtures/
     graphlookup_basic_test.py
     graphlookup_nested_array.py
     graphlookup_nested_dict.py

"""
import warnings
import pytest
import mongomock

from .fixtures import graphlookup_basic_test as test1
from .fixtures import graphlookup_nested_array as test2
from .fixtures import graphlookup_nested_dict as test3

warnings.simplefilter('ignore', DeprecationWarning)

@pytest.fixture(autouse=True, scope="function")
def somedb():
    """Database fixture for the tests
    """
    client = mongomock.MongoClient()
    _database = client['somedb']
    yield _database

def getdata(testcase):
    """Extract testcase elements from testcase
    """
    return testcase.data_a, testcase.data_b, testcase.query, testcase.expected

@pytest.mark.parametrize(
    "data_a,data_b,query,expected",
    [
        getdata(test1),
        getdata(test2),
        getdata(test3)
        ])
def test_graphlookup(somedb, data_a, data_b, query, expected): # pylint: disable=redefined-outer-name
    """Basic GraphLookup Test
    """
    somedb.a.insert_many(data_a)
    somedb.b.insert_many(data_b)
    actual = somedb.b.aggregate(query)
    actual = list(actual)
    # If this test fails it could be because we are comparing
    # dictionaries and lists. We need normalize the objects before
    # comparing.
    assert expected == actual
