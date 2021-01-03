"""
          UNIT TEST FOR $graphLookup WITH CONNECT FROM FIELD

The test cases are defined in the files:

- fixtures/
     graphlookup_basic_test.py
     graphlookup_nested_array.py
     graphlookup_nested_dict.py

"""
import warnings
from unittest import TestCase
import mongomock
from tests.diff import diff

from .fixtures import graphlookup_basic_test as test1
from .fixtures import graphlookup_nested_array as test2
from .fixtures import graphlookup_nested_dict as test3

warnings.simplefilter('ignore', DeprecationWarning)

def getdata(testcase):
    """Extract testcase elements from testcase
    """
    return testcase.data_a, testcase.data_b, testcase.query, testcase.expected

class GraphLookupAPITest(TestCase):
    """Test for $graphLookup withdotted ConnectFromField
    """

    def setUp(self):
        super(GraphLookupAPITest, self).setUp()
        self.client = mongomock.MongoClient()
        self.db = self.client['somedb']

    def test_graphlookup_basic(self):
        self.perform(test1)

    def test_graphlookup_nested_array(self):
        self.perform(test2)

    def test_graphlookup_nested_dict(self):
        self.perform(test3)

    def perform(self, testcase):
        data_a, data_b, query, expected = getdata(testcase)
        self.db.a.insert_many(data_a)
        self.db.b.insert_many(data_b)
        actual = self.db.b.aggregate(query)
        actual = list(actual)
        # the diff between expected and actual should be empty
        res = diff(expected, actual)
        self.assertEqual(res,[])
