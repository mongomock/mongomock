"""
    TESTCASE FOR GRAPHLOOKUP WITH CONNECT FROM FIELD

* This test cases connectfrom x.y where x is a dictionary.

* The testcase is taken from
      https://stackoverflow.com/questions/40989763/mongodb-graphlookup

* The inputs and the query are copy/pasted directly from the link
  above (with some cleanup)

* The expected output is formatted to match the pprint'ed output
  produced by mongomock.

* The elements are:

     - data_a: documents for database a
     - data_b: documents for database b
     - query: query for database b
     - expected: result expected from query execution

* The ISODate(x) helps to keep intact the original code from the above link.

"""

def ISODate(_x): # pylint: disable=invalid-name
    """Dummy function. Included to enable keeping the original data
    from the link mentioned above
    """
    return _x


data_b = [
    {"_id": 1, "name": "Dev"},
    {"_id": 2, "name": "Eliot", "reportsTo": {
        'name': "Dev", "from": ISODate("2016-01-01T00:00:00.000Z")}},
    {"_id": 3, "name": "Ron", "reportsTo": {'name': "Eliot",
                                            "from": ISODate("2016-01-01T00:00:00.000Z")}},
    {"_id": 4, "name": "Andrew", "reportsTo": {
        'name': "Eliot", "from": ISODate("2016-01-01T00:00:00.000Z")}},
    {"_id": 5, "name": "Asya", "reportsTo": {
        'name': "Ron", "from": ISODate("2016-01-01T00:00:00.000Z")}},
    {"_id": 6, "name": "Dan", "reportsTo": {'name': "Andrew",
                                            "from": ISODate("2016-01-01T00:00:00.000Z")}},
]

data_a = [{'_id':1, 'name':'x'}]

query = [
    {
        '$graphLookup': {
            'from': "b",
            'startWith': "$name",
            'connectFromField': "reportsTo.name",
            'connectToField': "name",
            'as': "reportingHierarchy"
        }
    }
]

expected = [{'_id': 1,
             'name': 'Dev',
             'reportingHierarchy': [{'_id': 1, 'name': 'Dev'}]},
            {'_id': 2,
             'name': 'Eliot',
             'reportingHierarchy': [{'_id': 2,
                                     'name': 'Eliot',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Dev'}},
                                    {'_id': 1, 'name': 'Dev'}],
             'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Dev'}},
            {'_id': 3,
             'name': 'Ron',
             'reportingHierarchy': [{'_id': 3,
                                     'name': 'Ron',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Eliot'}},
                                    {'_id': 2,
                                     'name': 'Eliot',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Dev'}},
                                    {'_id': 1, 'name': 'Dev'}],
             'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Eliot'}},
            {'_id': 4,
             'name': 'Andrew',
             'reportingHierarchy': [{'_id': 4,
                                     'name': 'Andrew',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Eliot'}},
                                    {'_id': 2,
                                     'name': 'Eliot',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Dev'}},
                                    {'_id': 1, 'name': 'Dev'}],
             'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Eliot'}},
            {'_id': 5,
             'name': 'Asya',
             'reportingHierarchy': [{'_id': 5,
                                     'name': 'Asya',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Ron'}},
                                    {'_id': 3,
                                     'name': 'Ron',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Eliot'}},
                                    {'_id': 2,
                                     'name': 'Eliot',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Dev'}},
                                    {'_id': 1, 'name': 'Dev'}],
             'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Ron'}},
            {'_id': 6,
             'name': 'Dan',
             'reportingHierarchy': [{'_id': 6,
                                     'name': 'Dan',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Andrew'}},
                                    {'_id': 4,
                                     'name': 'Andrew',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Eliot'}},
                                    {'_id': 2,
                                     'name': 'Eliot',
                                     'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'),
                                                   'name': 'Dev'}},
                                    {'_id': 1, 'name': 'Dev'}],
             'reportsTo': {'from': ISODate('2016-01-01T00:00:00.000Z'), 'name': 'Andrew'}}]
