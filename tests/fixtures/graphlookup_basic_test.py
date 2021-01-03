"""
    TESTCASE FOR GRAPHLOOKUP WITH CONNECT FROM FIELD

* This testcase has a simple connect from field without the dot operator.

* The test case is taken from
  https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/

* The inputs and the query are copy/pasted directly from the link
  above.

* The expected output is formatted to match the pprint'ed output
  produced by mongomock.

* The elements are:

     - data_a: documents for database a
     - data_b: documents for database b
     - query: query for database b
     - expected: result expected from query execution

"""

from collections import OrderedDict

data_a = [
    {'_id': 0, 'airport': 'JFK', 'connects': ['BOS', 'ORD']},
    {'_id': 1, 'airport': 'BOS', 'connects': ['JFK', 'PWM']},
    {'_id': 2, 'airport': 'ORD', 'connects': ['JFK']},
    {'_id': 3, 'airport': 'PWM', 'connects': ['BOS', 'LHR']},
    {'_id': 4, 'airport': 'LHR', 'connects': ['PWM']},
]

data_b = [
    {'_id': 1, 'name': 'Dev', 'nearestAirport': 'JFK'},
    {'_id': 2, 'name': 'Eliot', 'nearestAirport': 'JFK'},
    {'_id': 3, 'name': 'Jeff', 'nearestAirport': 'BOS'},
]

query = [
    {
        '$graphLookup': {
            'from': 'a',
            'startWith': '$nearestAirport',
            'connectFromField': 'connects',
            'connectToField': 'airport',
            'maxDepth': 2,
            'depthField': 'numConnections',
            'as': 'destinations'
        }
    }
]

expected = [{'_id': 1,
             'destinations': [OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects', ['BOS', 'ORD']),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects', ['JFK', 'PWM']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects', ['JFK']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects', ['BOS', 'LHR']),
                                           ('numConnections', 2)])],
             'name': 'Dev',
             'nearestAirport': 'JFK'},
            {'_id': 2,
             'destinations': [OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects', ['BOS', 'ORD']),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects', ['JFK', 'PWM']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects', ['JFK']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects', ['BOS', 'LHR']),
                                           ('numConnections', 2)])],
             'name': 'Eliot',
             'nearestAirport': 'JFK'},
            {'_id': 3,
             'destinations': [OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects', ['JFK', 'PWM']),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects', ['BOS', 'ORD']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects', ['BOS', 'LHR']),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects', ['JFK']),
                                           ('numConnections', 2)]),
                              OrderedDict([('_id', 4),
                                           ('airport', 'LHR'),
                                           ('connects', ['PWM']),
                                           ('numConnections', 2)])],
             'name': 'Jeff',
             'nearestAirport': 'BOS'}]
