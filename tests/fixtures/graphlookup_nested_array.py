"""
    TESTCASE FOR GRAPHLOOKUP WITH CONNECT FROM FIELD

* This test cases connectfrom x.y where x is an array.

* The test case is adaptaed from
  https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/

* The input is modified wrap a dictionary around the list of cities in
* And query is modified accordingly.
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
    {'_id': 0, 'airport': 'JFK', 'connects': [
        {'to': 'BOS', 'distance': 200}, {'to': 'ORD', 'distance': 800}]},
    {'_id': 1, 'airport': 'BOS', 'connects': [
        {'to': 'JFK', 'distance': 200}, {'to': 'PWM', 'distance': 2000}]},
    {'_id': 2, 'airport': 'ORD', 'connects': [{'to': 'JFK', 'distance': 800}]},
    {'_id': 3, 'airport': 'PWM', 'connects': [
        {'to': 'BOS', 'distance': 2000}, {'to': 'LHR', 'distance': 6000}]},
    {'_id': 4, 'airport': 'LHR', 'connects': [{'to': 'PWM', 'distance': 6000}]},
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
            'connectFromField': 'connects.to',
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
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'BOS'},
                                             {'distance': 800,
                                                'to': 'ORD'}]),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'JFK'},
                                             {'distance': 2000,
                                                'to': 'PWM'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects',
                                            [{'distance': 800,
                                              'to': 'JFK'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects',
                                            [{'distance': 2000,
                                              'to': 'BOS'},
                                             {'distance': 6000,
                                                'to': 'LHR'}]),
                                           ('numConnections', 2)])],
             'name': 'Dev',
             'nearestAirport': 'JFK'},
            {'_id': 2,
             'destinations': [OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'BOS'},
                                             {'distance': 800,
                                                'to': 'ORD'}]),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'JFK'},
                                             {'distance': 2000,
                                                'to': 'PWM'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects',
                                            [{'distance': 800,
                                              'to': 'JFK'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects',
                                            [{'distance': 2000,
                                              'to': 'BOS'},
                                             {'distance': 6000,
                                                'to': 'LHR'}]),
                                           ('numConnections', 2)])],
             'name': 'Eliot',
             'nearestAirport': 'JFK'},
            {'_id': 3,
             'destinations': [OrderedDict([('_id', 1),
                                           ('airport', 'BOS'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'JFK'},
                                             {'distance': 2000,
                                                'to': 'PWM'}]),
                                           ('numConnections', 0)]),
                              OrderedDict([('_id', 0),
                                           ('airport', 'JFK'),
                                           ('connects',
                                            [{'distance': 200,
                                              'to': 'BOS'},
                                             {'distance': 800,
                                                'to': 'ORD'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 3),
                                           ('airport', 'PWM'),
                                           ('connects',
                                            [{'distance': 2000,
                                              'to': 'BOS'},
                                             {'distance': 6000,
                                                'to': 'LHR'}]),
                                           ('numConnections', 1)]),
                              OrderedDict([('_id', 2),
                                           ('airport', 'ORD'),
                                           ('connects',
                                            [{'distance': 800,
                                              'to': 'JFK'}]),
                                           ('numConnections', 2)]),
                              OrderedDict([('_id', 4),
                                           ('airport', 'LHR'),
                                           ('connects',
                                            [{'distance': 6000,
                                              'to': 'PWM'}]),
                                           ('numConnections', 2)])],
             'name': 'Jeff',
             'nearestAirport': 'BOS'}]
