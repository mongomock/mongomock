"""Tests for geospatial operators: $geoIntersects, $geoWithin, $near, $nearSphere, $geoNear."""

import unittest

from mongomock_ng import MongoClient
from mongomock_ng import OperationFailure
from mongomock_ng.geospatial import haversine_distance
from mongomock_ng.geospatial import parse_geojson
from mongomock_ng.geospatial import point_in_polygon
from mongomock_ng.geospatial import validate_geojson


class GeospatialParsingTest(unittest.TestCase):
    """Test GeoJSON parsing and validation."""

    def test_parse_geojson_point(self):
        geo = parse_geojson({'type': 'Point', 'coordinates': [0, 0]})
        assert geo['type'] == 'Point'
        validate_geojson(geo)

    def test_parse_geojson_invalid_type(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'InvalidType', 'coordinates': []})

    def test_parse_geojson_point_out_of_range(self):
        geo = parse_geojson({'type': 'Point', 'coordinates': [200, 100]})
        with self.assertRaises(OperationFailure):
            validate_geojson(geo)

    def test_parse_geojson_polygon(self):
        geo = parse_geojson(
            {'type': 'Polygon', 'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]}
        )
        assert geo['type'] == 'Polygon'
        validate_geojson(geo)

    def test_parse_geojson_polygon_with_hole(self):
        geo = parse_geojson(
            {
                'type': 'Polygon',
                'coordinates': [
                    [[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]],
                    [[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]],
                ],
            }
        )
        validate_geojson(geo)

    def test_parse_geojson_polygon_not_closed(self):
        geo = parse_geojson(
            {'type': 'Polygon', 'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10]]]}
        )
        with self.assertRaises(OperationFailure):
            validate_geojson(geo)

    def test_parse_geojson_polygon_ring_closure_floating_point_tolerance(self):
        geo = parse_geojson(
            {'type': 'Polygon', 'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 1e-13]]]}
        )
        validate_geojson(geo)


class PointInPolygonTest(unittest.TestCase):
    """Test point-in-polygon ray casting."""

    def test_point_inside_polygon(self):
        polygon = [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        assert point_in_polygon((5, 5), polygon)

    def test_point_outside_polygon(self):
        polygon = [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        assert not point_in_polygon((20, 20), polygon)

    def test_point_on_boundary(self):
        polygon = [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        assert point_in_polygon((0, 0), polygon)
        assert point_in_polygon((5, 0), polygon)

    def test_point_in_polygon_with_hole(self):
        polygon = [
            [[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]],
            [[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]],
        ]
        assert point_in_polygon((2, 2), polygon)
        assert not point_in_polygon((10, 10), polygon)

    def test_point_in_polygon_vertical_edge(self):
        polygon = [[[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]]
        assert point_in_polygon((5, 5), polygon)


class DistanceTest(unittest.TestCase):
    """Test distance calculations."""

    def test_haversine_distance_zero(self):
        dist = haversine_distance(0, 0, 0, 0)
        self.assertAlmostEqual(dist, 0, places=5)

    def test_haversine_distance_known_values(self):
        dist = haversine_distance(0, 0, 1, 1)
        self.assertGreater(dist, 150000)
        self.assertLess(dist, 160000)

    def test_haversine_distance_symmetric(self):
        dist1 = haversine_distance(10, 20, 30, 40)
        dist2 = haversine_distance(30, 40, 10, 20)
        self.assertAlmostEqual(dist1, dist2, places=5)


class GeoIntersectsTest(unittest.TestCase):
    """Test $geoIntersects operator."""

    def setUp(self):
        self.client = MongoClient()
        self.db = self.client.test
        self.col = self.db.col
        self.col.drop()

    def test_geoIntersects_point_in_polygon(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [5, 5]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [20, 20]}})

        result = list(
            self.col.find(
                {
                    'loc': {
                        '$geoIntersects': {
                            '$geometry': {
                                'type': 'Polygon',
                                'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_geoIntersects_point_to_point(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [5, 5]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [6, 6]}})

        result = list(
            self.col.find(
                {'loc': {'$geoIntersects': {'$geometry': {'type': 'Point', 'coordinates': [5, 5]}}}}
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_geoIntersects_multipolygon(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [2, 2]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [12, 12]}})

        result = list(
            self.col.find(
                {
                    'loc': {
                        '$geoIntersects': {
                            '$geometry': {
                                'type': 'MultiPolygon',
                                'coordinates': [
                                    [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                                    [[[11, 11], [20, 11], [20, 20], [11, 20], [11, 11]]],
                                ],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(result), 2)

    def test_geoIntersects_invalid_geometry(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [5, 5]}})

        with self.assertRaises(OperationFailure):
            list(
                self.col.find(
                    {
                        'loc': {
                            '$geoIntersects': {
                                '$geometry': {'type': 'InvalidType', 'coordinates': []}
                            }
                        }
                    }
                )
            )


class GeoWithinTest(unittest.TestCase):
    """Test $geoWithin operator."""

    def setUp(self):
        self.client = MongoClient()
        self.db = self.client.test
        self.col = self.db.col
        self.col.drop()

    def test_geoWithin_point_in_polygon(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [5, 5]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [20, 20]}})

        result = list(
            self.col.find(
                {
                    'loc': {
                        '$geoWithin': {
                            '$geometry': {
                                'type': 'Polygon',
                                'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_geoWithin_point_excluded_from_hole(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [2, 2]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.find(
                {
                    'loc': {
                        '$geoWithin': {
                            '$geometry': {
                                'type': 'Polygon',
                                'coordinates': [
                                    [[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]],
                                    [[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]],
                                ],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)


class NearTest(unittest.TestCase):
    """Test $near operator."""

    def setUp(self):
        self.client = MongoClient()
        self.db = self.client.test
        self.col = self.db.col
        self.col.drop()

    def test_near_sorts_by_distance(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.find(
                {'loc': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}}}}
            )
        )
        self.assertEqual([d['_id'] for d in result], [1, 2, 3])

    def test_near_with_maxDistance(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.find(
                {
                    'loc': {
                        '$near': {
                            '$geometry': {'type': 'Point', 'coordinates': [0, 0]},
                            '$maxDistance': 200000,
                        }
                    }
                }
            )
        )
        self.assertEqual([d['_id'] for d in result], [1, 2])

    def test_near_with_minDistance(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.find(
                {
                    'loc': {
                        '$near': {
                            '$geometry': {'type': 'Point', 'coordinates': [0, 0]},
                            '$minDistance': 150000,
                        }
                    }
                }
            )
        )
        self.assertEqual([d['_id'] for d in result], [2, 3])

    def test_near_with_sort_overrides_distance_sort(self):
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})

        result = list(
            self.col.find(
                {'loc': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}}}}
            ).sort('_id', -1)
        )
        self.assertEqual([d['_id'] for d in result], [3, 2, 1])

    def test_nearSphere_uses_spherical_distance(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})

        result = list(
            self.col.find(
                {'loc': {'$nearSphere': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}}}}
            )
        )
        self.assertEqual(len(result), 2)

    def test_near_multiple_fields_uses_min_distance(self):
        self.col.insert_one(
            {
                '_id': 1,
                'loc1': {'type': 'Point', 'coordinates': [0, 0]},
                'loc2': {'type': 'Point', 'coordinates': [0, 0]},
            }
        )
        self.col.insert_one(
            {
                '_id': 2,
                'loc1': {'type': 'Point', 'coordinates': [1, 1]},
                'loc2': {'type': 'Point', 'coordinates': [0.5, 0.5]},
            }
        )

        result = list(
            self.col.find(
                {'loc1': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}}}}
            )
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['_id'], 1)
        self.assertEqual(result[1]['_id'], 2)

    def test_near_equal_distance_preserves_insertion_order(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [5, 5]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [5, 5]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [5, 5]}})

        result = list(
            self.col.find(
                {'loc': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [5, 5]}}}}
            )
        )
        self.assertEqual(len(result), 3)
        self.assertEqual([d['_id'] for d in result], [1, 2, 3])

    def test_near_skips_missing_location_field(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'name': 'no location'})

        result = list(
            self.col.find(
                {'loc': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}}}}
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_near_skips_non_point_geometry(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one(
            {'_id': 2, 'loc': {'type': 'LineString', 'coordinates': [[1, 1], [2, 2]]}}
        )

        result = list(
            self.col.find(
                {'loc': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}}}}
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_near_with_nested_field_path(self):
        self.col.insert_one({'_id': 1, 'geo': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'geo': {'type': 'Point', 'coordinates': [1, 1]}})

        result = list(
            self.col.find(
                {'geo': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}}}}
            )
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['_id'], 1)
        self.assertEqual(result[1]['_id'], 2)

    def test_near_zero_distance_exact_match(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})

        result = list(
            self.col.find(
                {'loc': {'$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}}}}
            )
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['_id'], 1)

    def test_near_distance_constraints_edge_case(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})

        result = list(
            self.col.find(
                {
                    'loc': {
                        '$near': {
                            '$geometry': {'type': 'Point', 'coordinates': [0, 0]},
                            '$minDistance': 155000,
                            '$maxDistance': 160000,
                        }
                    }
                }
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 2)

    def test_near_all_docs_beyond_maxDistance(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [10.5, 10.5]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [11, 11]}})

        result = list(
            self.col.find(
                {
                    'loc': {
                        '$near': {
                            '$geometry': {'type': 'Point', 'coordinates': [0, 0]},
                            '$maxDistance': 100000,
                        }
                    }
                }
            )
        )
        self.assertEqual(len(result), 0)


class GeoNearAggregationTest(unittest.TestCase):
    """Test $geoNear aggregation stage."""

    def setUp(self):
        self.client = MongoClient()
        self.db = self.client.test
        self.col = self.db.col
        self.col.drop()

    def test_geoNear_basic(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['_id'], 1)
        self.assertAlmostEqual(result[0]['distance'], 0, places=5)

    def test_geoNear_with_maxDistance(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                            'maxDistance': 200000,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 2)

    def test_geoNear_with_limit(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                            'limit': 1,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_geoNear_with_query(self):
        self.col.insert_one(
            {'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}, 'type': 'A'}
        )
        self.col.insert_one(
            {'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}, 'type': 'B'}
        )

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                            'query': {'type': 'B'},
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 2)

    def test_geoNear_missing_distanceField(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})

        with self.assertRaises(OperationFailure):
            list(
                self.col.aggregate(
                    [
                        {
                            '$geoNear': {
                                'near': {'type': 'Point', 'coordinates': [0, 0]},
                                'spherical': True,
                            }
                        }
                    ]
                )
            )

    def test_geoNear_missing_near(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})

        with self.assertRaises(OperationFailure):
            list(
                self.col.aggregate(
                    [
                        {
                            '$geoNear': {
                                'distanceField': 'distance',
                                'spherical': True,
                            }
                        }
                    ]
                )
            )

    def test_geoNear_with_minDistance(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                            'minDistance': 50000,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 2)
        self.assertIn(result[0]['_id'], [2, 3])

    def test_geoNear_with_minDistance_and_maxDistance(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                            'minDistance': 100000,
                            'maxDistance': 200000,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 2)

    def test_geoNear_with_key_field(self):
        self.col.insert_one({'_id': 1, 'custom_location': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'custom_location': {'type': 'Point', 'coordinates': [1, 1]}})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                            'key': 'custom_location',
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['_id'], 1)

    def test_geoNear_with_num_parameter(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        self.col.insert_one({'_id': 3, 'loc': {'type': 'Point', 'coordinates': [10, 10]}})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                            'num': 2,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 2)

    def test_geoNear_skips_non_point_docs(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one(
            {'_id': 2, 'loc': {'type': 'LineString', 'coordinates': [[1, 1], [2, 2]]}}
        )

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_geoNear_skips_missing_location(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'name': 'no location'})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_geoNear_skips_invalid_geometry(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [200, 200]}})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['_id'], 1)

    def test_geoNear_empty_collection(self):
        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 0)

    def test_geoNear_spherical_vs_planar(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})

        result_spherical = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                        }
                    }
                ]
            )
        )

        self.col.drop()
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})

        result_planar = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': False,
                        }
                    }
                ]
            )
        )

        self.assertEqual(len(result_spherical), 2)
        self.assertEqual(len(result_planar), 2)
        self.assertEqual(result_spherical[0]['_id'], result_planar[0]['_id'])

    def test_geoNear_legacy_coordinates(self):
        # $geoNear in aggregation requires GeoJSON format, not legacy [lon,lat]
        # Legacy coords in docs are silently skipped (treated as invalid geometry)
        self.col.insert_one({'_id': 1, 'loc': [0, 0]})
        self.col.insert_one({'_id': 2, 'loc': [1, 1]})

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                        }
                    }
                ]
            )
        )
        # Legacy format not supported by $geoNear, returns empty
        self.assertEqual(len(result), 0)

    def test_geoNear_preserves_doc_fields(self):
        self.col.insert_one(
            {'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}, 'name': 'A', 'value': 42}
        )
        self.col.insert_one(
            {'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}, 'name': 'B', 'value': 99}
        )

        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [0, 0]},
                            'distanceField': 'distance',
                            'spherical': True,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['name'], 'A')
        self.assertEqual(result[0]['value'], 42)
        self.assertIn('distance', result[0])
        self.assertGreaterEqual(result[0]['distance'], 0)


if __name__ == '__main__':
    unittest.main()
