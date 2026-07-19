"""Tests for geospatial operators: $geoIntersects, $geoWithin, $near, $nearSphere, $geoNear."""

import math
import unittest

from mongomock_ng import MongoClient
from mongomock_ng import OperationFailure
from mongomock_ng.geospatial import _collinear_overlap_interior
from mongomock_ng.geospatial import _collinear_segments_overlap
from mongomock_ng.geospatial import _euclidean_distance
from mongomock_ng.geospatial import _linestring_intersects_polygon
from mongomock_ng.geospatial import _on_segment_bounds
from mongomock_ng.geospatial import _orientation
from mongomock_ng.geospatial import _point_on_linestring
from mongomock_ng.geospatial import _point_on_segment
from mongomock_ng.geospatial import _points_equal
from mongomock_ng.geospatial import _polygon_edges
from mongomock_ng.geospatial import _segments_cross
from mongomock_ng.geospatial import _segments_cross_interior
from mongomock_ng.geospatial import _segments_intersect
from mongomock_ng.geospatial import _to_float_pairs
from mongomock_ng.geospatial import _validate_linestring_coords
from mongomock_ng.geospatial import _validate_point_coords
from mongomock_ng.geospatial import _validate_polygon_coords
from mongomock_ng.geospatial import extract_near_specs
from mongomock_ng.geospatial import geo_intersects
from mongomock_ng.geospatial import geo_within
from mongomock_ng.geospatial import haversine_distance
from mongomock_ng.geospatial import near_filter
from mongomock_ng.geospatial import parse_geojson
from mongomock_ng.geospatial import parse_near_spec
from mongomock_ng.geospatial import point_from_geojson
from mongomock_ng.geospatial import point_in_multipolygon
from mongomock_ng.geospatial import point_in_polygon
from mongomock_ng.geospatial import point_in_polygon_ray_casting
from mongomock_ng.geospatial import validate_coord_range
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
        self.col.create_index([('loc', '2dsphere')])

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
        self.col.create_index([('loc1', '2dsphere')])
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
        self.col.create_index([('geo', '2dsphere')])
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
        self.col.create_index([('loc', '2dsphere')])

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
        self.col.create_index([('custom_location', '2dsphere')])
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


class GeoIntersectsNonPointTest(unittest.TestCase):
    """Test $geoIntersects with non-Point document geometries."""

    def setUp(self):
        self.client = MongoClient()
        self.db = self.client.test
        self.col = self.db.col
        self.col.drop()
        self.col.create_index([('loc', '2dsphere')])

    def test_multipoint_some_inside(self):
        self.col.insert_one(
            {
                '_id': 1,
                'loc': {'type': 'MultiPoint', 'coordinates': [[5, 5], [20, 20]]},
            }
        )
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

    def test_multipoint_none_inside(self):
        self.col.insert_one(
            {
                '_id': 2,
                'loc': {'type': 'MultiPoint', 'coordinates': [[20, 20], [30, 30]]},
            }
        )
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
        self.assertEqual(len(result), 0)

    def test_linestring_crossing_polygon(self):
        self.col.insert_one(
            {
                '_id': 3,
                'loc': {'type': 'LineString', 'coordinates': [[-5, 5], [15, 5]]},
            }
        )
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
        self.assertEqual(result[0]['_id'], 3)

    def test_linestring_disjoint(self):
        self.col.insert_one(
            {
                '_id': 4,
                'loc': {'type': 'LineString', 'coordinates': [[20, 20], [30, 30]]},
            }
        )
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
        self.assertEqual(len(result), 0)

    def test_multilinestring_any_intersects(self):
        self.col.insert_one(
            {
                '_id': 5,
                'loc': {
                    'type': 'MultiLineString',
                    'coordinates': [
                        [[20, 20], [30, 30]],
                        [[-5, 5], [15, 5]],
                    ],
                },
            }
        )
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
        self.assertEqual(result[0]['_id'], 5)

    def test_multilinestring_all_disjoint(self):
        self.col.insert_one(
            {
                '_id': 6,
                'loc': {
                    'type': 'MultiLineString',
                    'coordinates': [
                        [[20, 20], [30, 30]],
                        [[-10, -10], [-5, -5]],
                    ],
                },
            }
        )
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
        self.assertEqual(len(result), 0)

    def test_polygon_overlapping(self):
        self.col.insert_one(
            {
                '_id': 7,
                'loc': {
                    'type': 'Polygon',
                    'coordinates': [[[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]]],
                },
            }
        )
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
        self.assertEqual(result[0]['_id'], 7)

    def test_polygon_disjoint(self):
        self.col.insert_one(
            {
                '_id': 8,
                'loc': {
                    'type': 'Polygon',
                    'coordinates': [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]],
                },
            }
        )
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
        self.assertEqual(len(result), 0)

    def test_multipolygon_any_intersects(self):
        self.col.insert_one(
            {
                '_id': 9,
                'loc': {
                    'type': 'MultiPolygon',
                    'coordinates': [
                        [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]],
                        [[[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]]],
                    ],
                },
            }
        )
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
        self.assertEqual(result[0]['_id'], 9)

    def test_multipolygon_all_disjoint(self):
        self.col.insert_one(
            {
                '_id': 10,
                'loc': {
                    'type': 'MultiPolygon',
                    'coordinates': [
                        [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]],
                        [[[50, 50], [60, 50], [60, 60], [50, 60], [50, 50]]],
                    ],
                },
            }
        )
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
        self.assertEqual(len(result), 0)

    def test_geometrycollection_has_point_inside(self):
        self.col.insert_one(
            {
                '_id': 11,
                'loc': {
                    'type': 'GeometryCollection',
                    'geometries': [
                        {'type': 'Point', 'coordinates': [5, 5]},
                        {'type': 'Point', 'coordinates': [20, 20]},
                    ],
                },
            }
        )
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
        self.assertEqual(result[0]['_id'], 11)

    def test_geometrycollection_no_intersect(self):
        self.col.insert_one(
            {
                '_id': 12,
                'loc': {
                    'type': 'GeometryCollection',
                    'geometries': [
                        {'type': 'Point', 'coordinates': [20, 20]},
                        {'type': 'Point', 'coordinates': [30, 30]},
                    ],
                },
            }
        )
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
        self.assertEqual(len(result), 0)


class GeoWithinNonPointTest(unittest.TestCase):
    """Test $geoWithin with non-Point document geometries."""

    def setUp(self):
        self.client = MongoClient()
        self.db = self.client.test
        self.col = self.db.col
        self.col.drop()
        self.col.create_index([('loc', '2dsphere')])

    def test_multipoint_all_inside(self):
        self.col.insert_one(
            {
                '_id': 101,
                'loc': {'type': 'MultiPoint', 'coordinates': [[2, 2], [5, 5], [8, 8]]},
            }
        )
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
        self.assertEqual(result[0]['_id'], 101)

    def test_multipoint_some_outside(self):
        self.col.insert_one(
            {
                '_id': 102,
                'loc': {'type': 'MultiPoint', 'coordinates': [[2, 2], [20, 20]]},
            }
        )
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
        self.assertEqual(len(result), 0)

    def test_linestring_within(self):
        self.col.insert_one(
            {
                '_id': 103,
                'loc': {'type': 'LineString', 'coordinates': [[2, 2], [5, 5], [8, 2]]},
            }
        )
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
        self.assertEqual(result[0]['_id'], 103)

    def test_polygon_within(self):
        self.col.insert_one(
            {
                '_id': 104,
                'loc': {
                    'type': 'Polygon',
                    'coordinates': [[[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]]],
                },
            }
        )
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
        self.assertEqual(result[0]['_id'], 104)

    def test_multipolygon_all_within(self):
        self.col.insert_one(
            {
                '_id': 105,
                'loc': {
                    'type': 'MultiPolygon',
                    'coordinates': [
                        [[[1, 1], [3, 1], [3, 3], [1, 3], [1, 1]]],
                        [[[6, 6], [9, 6], [9, 9], [6, 9], [6, 6]]],
                    ],
                },
            }
        )
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
        self.assertEqual(result[0]['_id'], 105)


class GeoNearAggregationEdgeCaseTest(unittest.TestCase):
    """$geoNear edge cases."""

    def setUp(self):
        self.client = MongoClient()
        self.db = self.client.test
        self.col = self.db.col
        self.col.drop()
        self.col.create_index([('loc', '2dsphere')])

    def test_geoNear_non_dict_options(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        with self.assertRaises(OperationFailure):
            list(self.col.aggregate([{'$geoNear': 'not an object'}]))

    def test_geoNear_non_point_geometry_error(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        with self.assertRaises(OperationFailure):
            list(
                self.col.aggregate(
                    [
                        {
                            '$geoNear': {
                                'near': {
                                    'type': 'Polygon',
                                    'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 0]]],
                                },
                                'distanceField': 'distance',
                                'spherical': True,
                            }
                        }
                    ]
                )
            )

    def test_geoNear_legacy_array_near(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': [0, 0],
                            'distanceField': 'distance',
                            'spherical': False,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['_id'], 1)
        self.assertAlmostEqual(result[0]['distance'], 0, places=5)

    def test_geoNear_distance_multiplier(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        self.col.insert_one({'_id': 2, 'loc': {'type': 'Point', 'coordinates': [0, 1]}})
        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': [0, 0],
                            'distanceField': 'distance',
                            'spherical': False,
                            'distanceMultiplier': 3.0,
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 2)
        self.assertAlmostEqual(result[1]['distance'], 3.0, places=5)
        self.assertAlmostEqual(result[0]['distance'], 0.0, places=5)

    def test_geoNear_include_locs(self):
        self.col.insert_one({'_id': 1, 'loc': {'type': 'Point', 'coordinates': [-73.97, 40.77]}})
        result = list(
            self.col.aggregate(
                [
                    {
                        '$geoNear': {
                            'near': {'type': 'Point', 'coordinates': [-73.97, 40.77]},
                            'distanceField': 'distance',
                            'spherical': True,
                            'includeLocs': 'locationObj',
                        }
                    }
                ]
            )
        )
        self.assertEqual(len(result), 1)
        self.assertIn('locationObj', result[0])
        self.assertEqual(
            result[0]['locationObj'],
            {'type': 'Point', 'coordinates': [-73.97, 40.77]},
        )


class GeoWithinQueryGeometryTest(unittest.TestCase):
    """Test $geoWithin/$geoIntersects queries with various query geometry types."""

    def setUp(self):
        self.client = MongoClient()
        self.db = self.client.test
        self.col = self.db.col
        self.col.drop()

    def test_geoWithin_with_query_multipoint(self):
        self.col.insert_one({'loc': {'type': 'Point', 'coordinates': [2, 2]}})
        docs = list(
            self.col.find(
                {
                    'loc': {
                        '$geoWithin': {
                            '$geometry': {
                                'type': 'MultiPoint',
                                'coordinates': [[1, 1], [2, 2], [3, 3]],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(docs), 1)

    def test_geoWithin_with_query_point(self):
        self.col.insert_one({'loc': {'type': 'Point', 'coordinates': [1, 1]}})
        docs = list(
            self.col.find(
                {
                    'loc': {
                        '$geoWithin': {
                            '$geometry': {
                                'type': 'Point',
                                'coordinates': [1, 1],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(docs), 1)

    def test_geoWithin_with_query_multipolygon(self):
        self.col.insert_one({'loc': {'type': 'Point', 'coordinates': [25, 25]}})
        docs = list(
            self.col.find(
                {
                    'loc': {
                        '$geoWithin': {
                            '$geometry': {
                                'type': 'MultiPolygon',
                                'coordinates': [
                                    [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                                    [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]],
                                ],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(docs), 1)

    def test_geoIntersects_with_query_multipoint(self):
        self.col.insert_one({'loc': {'type': 'Point', 'coordinates': [2, 2]}})
        docs = list(
            self.col.find(
                {
                    'loc': {
                        '$geoIntersects': {
                            '$geometry': {
                                'type': 'MultiPoint',
                                'coordinates': [[1, 1], [2, 2], [3, 3]],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(docs), 1)

    def test_geoIntersects_with_query_linestring(self):
        self.col.insert_one({'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        docs = list(
            self.col.find(
                {
                    'loc': {
                        '$geoIntersects': {
                            '$geometry': {
                                'type': 'LineString',
                                'coordinates': [[-1, -1], [1, 1]],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(docs), 1)

    def test_geoIntersects_with_query_multilinestring(self):
        self.col.insert_one({'loc': {'type': 'Point', 'coordinates': [0, 0]}})
        docs = list(
            self.col.find(
                {
                    'loc': {
                        '$geoIntersects': {
                            '$geometry': {
                                'type': 'MultiLineString',
                                'coordinates': [
                                    [[-10, -10], [-5, -5]],
                                    [[-1, -1], [1, 1]],
                                ],
                            }
                        }
                    }
                }
            )
        )
        self.assertEqual(len(docs), 1)


class ValidationEdgeCaseTest(unittest.TestCase):
    """Test edge cases in GeoJSON parsing and validation."""

    def test_parse_geojson_geometry_collection(self):
        geo = parse_geojson(
            {
                'type': 'GeometryCollection',
                'geometries': [
                    {'type': 'Point', 'coordinates': [1, 2]},
                    {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]},
                    {'type': 'Polygon', 'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
                ],
            }
        )
        self.assertEqual(geo['type'], 'GeometryCollection')
        self.assertEqual(len(geo['geometries']), 3)
        validate_geojson(geo)

    def test_parse_geojson_geometry_collection_invalid_geometries(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'GeometryCollection', 'geometries': 'not_a_list'})

    def test_parse_geojson_non_array_coordinates(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'Point', 'coordinates': 'not_an_array'})

    def test_parse_geojson_wrong_depth(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'Point', 'coordinates': [[0, 0]]})

    def test_validate_coord_range_non_numeric_lon(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range('abc', 0)

    def test_validate_coord_range_non_numeric_lat(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(0, 'abc')

    def test_validate_coord_range_lat_out_of_range(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(0, -100)

    def test_validate_geojson_multi_point(self):
        geo = parse_geojson(
            {
                'type': 'MultiPoint',
                'coordinates': [[0, 0], [1, 1], [2, 2]],
            }
        )
        validate_geojson(geo)

    def test_validate_geojson_multi_line_string(self):
        geo = parse_geojson(
            {
                'type': 'MultiLineString',
                'coordinates': [
                    [[0, 0], [1, 1]],
                    [[2, 2], [3, 3]],
                ],
            }
        )
        validate_geojson(geo)

    def test_validate_geojson_geometry_collection(self):
        geo = parse_geojson(
            {
                'type': 'GeometryCollection',
                'geometries': [
                    {'type': 'Point', 'coordinates': [10, 20]},
                ],
            }
        )
        validate_geojson(geo)

    def test_validate_point_too_few_coords(self):
        with self.assertRaises(OperationFailure):
            _validate_point_coords([0])

    def test_validate_linestring_too_few_points(self):
        with self.assertRaises(OperationFailure):
            _validate_linestring_coords([[0, 0]])

    def test_validate_polygon_empty_rings(self):
        with self.assertRaises(OperationFailure):
            _validate_polygon_coords([])

    def test_validate_polygon_ring_too_few_points(self):
        with self.assertRaises(OperationFailure):
            _validate_polygon_coords([[[0, 0], [1, 1], [2, 2]]])


class PureFunctionTest(unittest.TestCase):
    """Unit tests for pure (no-side-effect) helper functions."""

    # -- point_from_geojson --

    def test_point_from_geojson(self):
        pt = point_from_geojson({'type': 'Point', 'coordinates': [30, 40]})
        self.assertEqual(pt, (30, 40))

    def test_point_from_geojson_with_z(self):
        pt = point_from_geojson({'type': 'Point', 'coordinates': [30, 40, 200]})
        self.assertEqual(pt, (30, 40))

    # -- point_in_polygon_ray_casting, point_in_polygon, point_in_multipolygon --

    def test_ray_casting_inside(self):
        ring = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        self.assertTrue(point_in_polygon_ray_casting((5, 5), ring))

    def test_ray_casting_outside(self):
        ring = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        self.assertFalse(point_in_polygon_ray_casting((20, 20), ring))

    def test_ray_casting_on_vertex(self):
        ring = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        self.assertTrue(point_in_polygon_ray_casting((0, 0), ring))

    def test_ray_casting_on_horizontal_edge(self):
        ring = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        self.assertTrue(point_in_polygon_ray_casting((5, 0), ring))

    def test_ray_casting_vertical_line_intersection(self):
        ring = [(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]
        self.assertTrue(point_in_polygon_ray_casting((5, 5), ring))
        self.assertTrue(point_in_polygon_ray_casting((0, 5), ring))

    def test_point_in_polygon_simple(self):
        coords = [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        self.assertTrue(point_in_polygon((5, 5), coords))
        self.assertFalse(point_in_polygon((20, 20), coords))

    def test_point_in_polygon_with_hole(self):
        coords = [
            [[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]],
            [[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]],
        ]
        self.assertTrue(point_in_polygon((2, 2), coords))
        self.assertFalse(point_in_polygon((10, 10), coords))

    def test_point_in_polygon_on_hole_edge(self):
        coords = [
            [[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]],
            [[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]],
        ]
        self.assertFalse(point_in_polygon((5, 5), coords))

    def test_point_in_multipolygon(self):
        coords = [
            [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
            [[[15, 15], [25, 15], [25, 25], [15, 25], [15, 15]]],
        ]
        self.assertTrue(point_in_multipolygon((5, 5), coords))
        self.assertTrue(point_in_multipolygon((20, 20), coords))
        self.assertFalse(point_in_multipolygon((12, 12), coords))

    # -- _to_float_pairs --

    def test_to_float_pairs(self):
        result = _to_float_pairs([[1, 2], [3, 4]])
        self.assertEqual(result, [(1.0, 2.0), (3.0, 4.0)])

    def test_to_float_pairs_with_z(self):
        result = _to_float_pairs([[1, 2, 99], [3, 4, 88]])
        self.assertEqual(result, [(1.0, 2.0), (3.0, 4.0)])

    def test_to_float_pairs_promotes_int(self):
        result = _to_float_pairs([[0, 0]])
        self.assertEqual(result, [(0.0, 0.0)])
        self.assertIsInstance(result[0][0], float)

    # -- _points_equal, _point_on_segment, _point_on_linestring --

    def test_points_equal_exact(self):
        self.assertTrue(_points_equal((3.0, 4.0), (3.0, 4.0)))

    def test_points_equal_within_tolerance(self):
        self.assertTrue(_points_equal((1.0, 2.0), (1.0 + 1e-13, 2.0 + 1e-13)))

    def test_points_equal_different(self):
        self.assertFalse(_points_equal((1.0, 2.0), (3.0, 4.0)))

    def test_points_equal_barely_outside_tolerance(self):
        self.assertFalse(_points_equal((0, 0), (1e-11, 0)))

    def test_point_on_segment_midpoint(self):
        self.assertTrue(_point_on_segment(5, 5, 0, 0, 10, 10))

    def test_point_on_segment_endpoint(self):
        self.assertTrue(_point_on_segment(0, 0, 0, 0, 10, 10))

    def test_point_on_segment_not_on_line(self):
        self.assertFalse(_point_on_segment(5, 6, 0, 0, 10, 10))

    def test_point_on_segment_beyond(self):
        self.assertFalse(_point_on_segment(20, 20, 0, 0, 10, 10))

    def test_point_on_segment_before(self):
        self.assertFalse(_point_on_segment(-5, -5, 0, 0, 10, 10))

    def test_point_on_segment_degenerate_zero_length(self):
        self.assertTrue(_point_on_segment(3, 3, 3, 3, 3, 3))
        self.assertFalse(_point_on_segment(4, 4, 3, 3, 3, 3))

    def test_point_on_segment_horizontal(self):
        self.assertTrue(_point_on_segment(5, 0, 0, 0, 10, 0))

    def test_point_on_segment_vertical(self):
        self.assertTrue(_point_on_segment(0, 5, 0, 0, 0, 10))

    def test_point_on_segment_full_segment(self):
        self.assertFalse(_point_on_segment(11, 11, 0, 0, 10, 10))

    def test_point_on_linestring_on_second_segment(self):
        ls = [(0, 0), (10, 0), (10, 10)]
        self.assertTrue(_point_on_linestring((10, 5), ls))

    def test_point_on_linestring_not_found(self):
        ls = [(0, 0), (10, 0), (10, 10)]
        self.assertFalse(_point_on_linestring((5, 5), ls))

    def test_point_on_linestring_on_vertex(self):
        ls = [(0, 0), (10, 0), (10, 10)]
        self.assertTrue(_point_on_linestring((10, 10), ls))

    # -- _orientation, _on_segment_bounds --

    def test_orientation_collinear(self):
        self.assertEqual(_orientation((0, 0), (10, 10), (5, 5)), 0)

    def test_orientation_counter_clockwise(self):
        self.assertEqual(_orientation((0, 0), (10, 0), (10, 10)), 1)

    def test_orientation_clockwise(self):
        self.assertEqual(_orientation((0, 0), (10, 0), (10, -10)), 2)

    def test_orientation_collinear_near_tolerance(self):
        val = 1e-13
        self.assertEqual(_orientation((0, 0), (10, 10), (5 + val, 5 + val)), 0)

    def test_on_segment_bounds_within(self):
        self.assertTrue(_on_segment_bounds((0, 0), (10, 10), (5, 5)))

    def test_on_segment_bounds_outside(self):
        self.assertFalse(_on_segment_bounds((0, 0), (10, 10), (20, 20)))

    def test_on_segment_bounds_at_endpoint(self):
        self.assertTrue(_on_segment_bounds((0, 0), (10, 10), (0, 0)))

    def test_on_segment_bounds_near_tolerance(self):
        self.assertTrue(_on_segment_bounds((0, 0), (10, 10), (-1e-13, -1e-13)))

    # -- _segments_intersect --

    def test_segments_intersect_cross(self):
        self.assertTrue(_segments_intersect((0, 0), (10, 10), (0, 10), (10, 0)))

    def test_segments_intersect_disjoint(self):
        self.assertFalse(_segments_intersect((0, 0), (5, 5), (10, 10), (15, 15)))

    def test_segments_intersect_share_endpoint(self):
        self.assertTrue(_segments_intersect((0, 0), (5, 5), (5, 5), (10, 0)))

    def test_segments_intersect_collinear_overlap(self):
        self.assertTrue(_segments_intersect((0, 0), (10, 10), (5, 5), (15, 15)))

    def test_segments_intersect_collinear_no_overlap(self):
        self.assertFalse(_segments_intersect((0, 0), (5, 5), (10, 10), (15, 15)))

    def test_segments_intersect_parallel_no_intersect(self):
        self.assertFalse(_segments_intersect((0, 0), (5, 0), (0, 5), (5, 5)))

    def test_segments_intersect_touching_at_endpoint(self):
        self.assertTrue(_segments_intersect((0, 0), (5, 5), (5, 5), (10, 0)))

    # -- _segments_cross --

    def test_segments_cross_proper(self):
        self.assertTrue(_segments_cross((0, 0), (10, 10), (0, 10), (10, 0)))

    def test_segments_cross_disjoint(self):
        self.assertFalse(_segments_cross((0, 0), (5, 5), (10, 10), (15, 15)))

    def test_segments_cross_collinear_overlap(self):
        self.assertTrue(_segments_cross((0, 0), (10, 10), (5, 5), (15, 15)))

    def test_segments_cross_share_endpoint_only(self):
        self.assertTrue(_segments_cross((0, 0), (5, 5), (5, 5), (10, 10)))

    # -- _segments_cross_interior --

    def test_segments_cross_interior_proper(self):
        self.assertTrue(_segments_cross_interior((0, 0), (10, 10), (0, 10), (10, 0)))

    def test_segments_cross_interior_disjoint(self):
        self.assertFalse(_segments_cross_interior((0, 0), (5, 5), (10, 10), (15, 15)))

    def test_segments_cross_interior_rejects_endpoint_touch(self):
        self.assertFalse(_segments_cross_interior((0, 0), (5, 5), (5, 5), (10, 0)))

    def test_segments_cross_interior_collinear_overlap_interior(self):
        self.assertTrue(_segments_cross_interior((0, 0), (10, 10), (3, 3), (7, 7)))

    def test_segments_cross_interior_collinear_touching_at_edge(self):
        self.assertFalse(_segments_cross_interior((0, 0), (5, 5), (5, 5), (10, 10)))

    # -- _collinear_segments_overlap --

    def test_collinear_overlap_yes(self):
        self.assertTrue(_collinear_segments_overlap((0, 0), (10, 10), (5, 5), (15, 15)))

    def test_collinear_overlap_no(self):
        self.assertFalse(_collinear_segments_overlap((0, 0), (5, 5), (10, 10), (15, 15)))

    def test_collinear_overlap_touching_endpoint(self):
        self.assertTrue(_collinear_segments_overlap((0, 0), (5, 5), (5, 5), (10, 10)))

    def test_collinear_overlap_identical(self):
        self.assertTrue(_collinear_segments_overlap((0, 0), (10, 10), (0, 0), (10, 10)))

    # -- _collinear_overlap_interior --

    def test_collinear_overlap_interior_yes(self):
        self.assertTrue(_collinear_overlap_interior((0, 0), (10, 10), (3, 3), (7, 7)))

    def test_collinear_overlap_interior_no_touching_at_endpoints(self):
        self.assertFalse(_collinear_overlap_interior((0, 0), (10, 10), (10, 10), (20, 20)))

    def test_collinear_overlap_interior_partial_overlap(self):
        self.assertTrue(_collinear_overlap_interior((0, 0), (10, 10), (5, 5), (15, 15)))

    def test_collinear_overlap_interior_contained(self):
        self.assertTrue(_collinear_overlap_interior((0, 0), (10, 10), (2, 2), (8, 8)))

    # -- _polygon_edges, _linestring_intersects_polygon --

    def test_polygon_edges_simple_ring(self):
        edges = _polygon_edges([[[0, 0], [10, 0], [10, 10], [0, 0]]])
        self.assertEqual(len(edges), 3)
        self.assertIn(((0.0, 0.0), (10.0, 0.0)), edges)
        self.assertIn(((10.0, 0.0), (10.0, 10.0)), edges)
        self.assertIn(((10.0, 10.0), (0.0, 0.0)), edges)

    def test_polygon_edges_with_hole(self):
        edges = _polygon_edges(
            [
                [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
                [[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]],
            ]
        )
        self.assertEqual(len(edges), 8)

    def test_linestring_intersects_polygon_inside(self):
        coords = [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        self.assertTrue(_linestring_intersects_polygon([(5, 5), (15, 5)], coords))

    def test_linestring_intersects_polygon_crossing_edge(self):
        coords = [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        self.assertTrue(_linestring_intersects_polygon([(-5, 5), (15, 5)], coords))

    def test_linestring_intersects_polygon_outside(self):
        coords = [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        self.assertFalse(_linestring_intersects_polygon([(20, 20), (30, 30)], coords))

    def test_linestring_intersects_polygon_touching_edge(self):
        coords = [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]
        self.assertTrue(_linestring_intersects_polygon([(5, 0), (15, 5)], coords))

    # -- _euclidean_distance --

    def test_euclidean_distance_zero(self):
        self.assertEqual(_euclidean_distance(0, 0, 0, 0), 0.0)

    def test_euclidean_distance_positive(self):
        d = _euclidean_distance(0, 0, 3, 4)
        self.assertAlmostEqual(d, 5.0)

    def test_euclidean_distance_negative_coords(self):
        d = _euclidean_distance(-1, -1, 2, 3)
        self.assertAlmostEqual(d, 5.0)

    def test_euclidean_distance_symmetric(self):
        d1 = _euclidean_distance(1, 2, 4, 6)
        d2 = _euclidean_distance(4, 6, 1, 2)
        self.assertAlmostEqual(d1, d2)

    def test_euclidean_distance_large_values(self):
        d = _euclidean_distance(-180, -90, 180, 90)
        self.assertAlmostEqual(d, math.sqrt(360**2 + 180**2))

    # -- parse_near_spec --

    def test_parse_near_spec_legacy_array(self):
        result = parse_near_spec([10, 20])
        self.assertEqual(result['query_point'], (10.0, 20.0))
        self.assertIsNone(result['max_distance'])
        self.assertIsNone(result['min_distance'])
        self.assertFalse(result['spherical'])

    def test_parse_near_spec_legacy_tuple(self):
        result = parse_near_spec((30, 40))
        self.assertEqual(result['query_point'], (30.0, 40.0))

    def test_parse_near_spec_geojson_point(self):
        result = parse_near_spec(
            {
                '$geometry': {'type': 'Point', 'coordinates': [10, 20]},
            }
        )
        self.assertEqual(result['query_point'], (10.0, 20.0))
        self.assertTrue(result['spherical'])

    def test_parse_near_spec_geojson_with_max_distance(self):
        result = parse_near_spec(
            {
                '$geometry': {'type': 'Point', 'coordinates': [0, 0]},
                '$maxDistance': 1000,
            }
        )
        self.assertEqual(result['max_distance'], 1000)

    def test_parse_near_spec_geojson_with_min_distance(self):
        result = parse_near_spec(
            {
                '$geometry': {'type': 'Point', 'coordinates': [0, 0]},
                '$minDistance': 500,
            }
        )
        self.assertEqual(result['min_distance'], 500)

    def test_parse_near_spec_geojson_implicit(self):
        result = parse_near_spec(
            {
                'type': 'Point',
                'coordinates': [5, 5],
            }
        )
        self.assertEqual(result['query_point'], (5.0, 5.0))

    def test_parse_near_spec_legacy_too_few_coords(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec([10])

    def test_parse_near_spec_legacy_out_of_range(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec([200, 0])

    def test_parse_near_spec_invalid_type(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec(123)

    def test_parse_near_spec_geojson_not_point(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec({'$geometry': {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]}})

    def test_parse_near_spec_no_geometry_key(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec({'coordinates': [0, 0]})

    # -- parse_geojson --

    def test_parse_geojson_not_dict(self):
        with self.assertRaises(OperationFailure):
            parse_geojson([1, 2, 3])

    def test_parse_geojson_invalid_type(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'InvalidType', 'coordinates': []})

    def test_parse_geojson_point(self):
        result = parse_geojson({'type': 'Point', 'coordinates': [10, 20]})
        self.assertEqual(result['type'], 'Point')
        self.assertEqual(result['coordinates'], [10, 20])

    def test_parse_geojson_geometry_collection(self):
        result = parse_geojson(
            {
                'type': 'GeometryCollection',
                'geometries': [
                    {'type': 'Point', 'coordinates': [1, 2]},
                    {'type': 'Point', 'coordinates': [3, 4]},
                ],
            }
        )
        self.assertEqual(result['type'], 'GeometryCollection')
        self.assertEqual(len(result['geometries']), 2)

    def test_parse_geojson_geometry_collection_non_list(self):
        with self.assertRaises(OperationFailure):
            parse_geojson(
                {
                    'type': 'GeometryCollection',
                    'geometries': 'not_a_list',
                }
            )

    def test_parse_geojson_missing_coordinates(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'Point', 'coordinates': None})

    def test_parse_geojson_wrong_depth(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'Point', 'coordinates': [[0, 0]]})

    # -- validate_coord_range --

    def test_validate_coord_range_valid(self):
        validate_coord_range(0, 0)
        validate_coord_range(-180, -90)
        validate_coord_range(180, 90)

    def test_validate_coord_range_lon_too_low(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(-181, 0)

    def test_validate_coord_range_lon_too_high(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(181, 0)

    def test_validate_coord_range_lat_too_low(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(0, -91)

    def test_validate_coord_range_lat_too_high(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(0, 91)

    def test_validate_coord_range_non_number_lon(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range('abc', 0)

    def test_validate_coord_range_non_number_lat(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(0, None)

    # -- validate_geojson --

    def test_validate_geojson_point_valid(self):
        validate_geojson({'type': 'Point', 'coordinates': [10, 20]})

    def test_validate_geojson_point_out_of_range(self):
        with self.assertRaises(OperationFailure):
            validate_geojson({'type': 'Point', 'coordinates': [200, 100]})

    def test_validate_geojson_point_fewer_than_2_coords(self):
        with self.assertRaises(OperationFailure):
            validate_geojson({'type': 'Point', 'coordinates': [10]})

    def test_validate_geojson_linestring_valid(self):
        validate_geojson(
            {
                'type': 'LineString',
                'coordinates': [[0, 0], [10, 10]],
            }
        )

    def test_validate_geojson_linestring_too_few_points(self):
        with self.assertRaises(OperationFailure):
            validate_geojson(
                {
                    'type': 'LineString',
                    'coordinates': [[0, 0]],
                }
            )

    def test_validate_geojson_polygon_valid(self):
        validate_geojson(
            {
                'type': 'Polygon',
                'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
            }
        )

    def test_validate_geojson_polygon_not_closed(self):
        with self.assertRaises(OperationFailure):
            validate_geojson(
                {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10]]],
                }
            )

    def test_validate_geojson_polygon_empty_rings(self):
        with self.assertRaises(OperationFailure):
            validate_geojson(
                {
                    'type': 'Polygon',
                    'coordinates': [],
                }
            )

    def test_validate_geojson_polygon_ring_too_few_points(self):
        with self.assertRaises(OperationFailure):
            validate_geojson(
                {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [1, 1], [2, 2]]],
                }
            )

    def test_validate_geojson_multipoint(self):
        validate_geojson(
            {
                'type': 'MultiPoint',
                'coordinates': [[0, 0], [10, 10]],
            }
        )

    def test_validate_geojson_multipoint_invalid(self):
        with self.assertRaises(OperationFailure):
            validate_geojson(
                {
                    'type': 'MultiPoint',
                    'coordinates': [[0, 0], [10]],
                }
            )

    def test_validate_geojson_multilinestring(self):
        validate_geojson(
            {
                'type': 'MultiLineString',
                'coordinates': [[[0, 0], [1, 1]], [[2, 2], [3, 3]]],
            }
        )

    def test_validate_geojson_multilinestring_invalid(self):
        with self.assertRaises(OperationFailure):
            validate_geojson(
                {
                    'type': 'MultiLineString',
                    'coordinates': [[[0, 0], [1, 1]], [[2]]],
                }
            )

    def test_validate_geojson_multipolygon(self):
        validate_geojson(
            {
                'type': 'MultiPolygon',
                'coordinates': [
                    [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                ],
            }
        )

    def test_validate_geojson_multipolygon_not_closed(self):
        with self.assertRaises(OperationFailure):
            validate_geojson(
                {
                    'type': 'MultiPolygon',
                    'coordinates': [
                        [[[0, 0], [1, 0], [1, 1], [0, 1]]],
                    ],
                }
            )

    def test_validate_geojson_geometry_collection(self):
        validate_geojson(
            {
                'type': 'GeometryCollection',
                'geometries': [
                    {'type': 'Point', 'coordinates': [0, 0]},
                    {'type': 'LineString', 'coordinates': [[1, 1], [2, 2]]},
                ],
            }
        )

    def test_validate_geojson_geometry_collection_with_invalid_child(self):
        with self.assertRaises(OperationFailure):
            validate_geojson(
                {
                    'type': 'GeometryCollection',
                    'geometries': [
                        {'type': 'Point', 'coordinates': [200, 100]},
                    ],
                }
            )

    def test_validate_geojson_polygon_closure_with_tolerance(self):
        validate_geojson(
            {
                'type': 'Polygon',
                'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 1e-13]]],
            }
        )


class GeospatialValidationTest(unittest.TestCase):
    """Group 1+2: Validation errors and validate_geojson branches."""

    def test_geometrycollection_non_list_geometries(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'GeometryCollection', 'geometries': 'not_a_list'})

    def test_coordinates_not_list(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'Point', 'coordinates': 'not_a_list'})

    def test_empty_list_depth(self):
        """_depth([]) hits empty list branch at line 68."""
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'Polygon', 'coordinates': []})

    def test_wrong_coordinate_depth(self):
        with self.assertRaises(OperationFailure):
            parse_geojson({'type': 'Point', 'coordinates': [[1, 2]]})

    def test_lon_not_number(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range('not_a_number', 0)

    def test_lat_not_number(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(0, 'not_a_number')

    def test_lon_out_of_range_positive(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(200, 0)

    def test_lon_out_of_range_negative(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(-200, 0)

    def test_lat_out_of_range_positive(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(0, 100)

    def test_lat_out_of_range_negative(self):
        with self.assertRaises(OperationFailure):
            validate_coord_range(0, -100)

    def test_point_less_than_2_coords(self):
        geo = parse_geojson({'type': 'Point', 'coordinates': [1]})
        with self.assertRaises(OperationFailure):
            validate_geojson(geo)

    def test_linestring_less_than_2_points(self):
        geo = parse_geojson({'type': 'LineString', 'coordinates': [[1, 2]]})
        with self.assertRaises(OperationFailure):
            validate_geojson(geo)

    def test_polygon_empty_coords(self):
        with self.assertRaises(OperationFailure):
            _validate_polygon_coords([])

    def test_polygon_ring_less_than_4_points(self):
        with self.assertRaises(OperationFailure):
            _validate_polygon_coords([[[0, 0], [1, 1], [2, 2]]])

    def test_validate_geojson_linestring(self):
        geo = parse_geojson({'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]})
        validate_geojson(geo)

    def test_validate_geojson_multipoint(self):
        geo = parse_geojson({'type': 'MultiPoint', 'coordinates': [[0, 0], [1, 1]]})
        validate_geojson(geo)

    def test_validate_geojson_multilinestring(self):
        geo = parse_geojson(
            {
                'type': 'MultiLineString',
                'coordinates': [[[0, 0], [1, 1]], [[2, 2], [3, 3]]],
            }
        )
        validate_geojson(geo)

    def test_validate_geojson_multipolygon(self):
        geo = parse_geojson(
            {
                'type': 'MultiPolygon',
                'coordinates': [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]],
            }
        )
        validate_geojson(geo)


class NearParseEdgeCaseTest(unittest.TestCase):
    """Group 3: $near/$nearSphere parse_near_spec edge cases."""

    def test_parse_near_spec_legacy_array_too_short(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec([1])

    def test_parse_near_spec_invalid_type(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec('invalid')

    def test_parse_near_spec_geometry_not_point(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec({'$geometry': {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]}})

    def test_parse_near_spec_type_not_point(self):
        with self.assertRaises(OperationFailure):
            parse_near_spec({'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]})

    def test_parse_near_spec_legacy_valid(self):
        """Legacy array [lon, lat] success path (lines 623-625)."""
        result = parse_near_spec([0, 0])
        self.assertEqual(result['query_point'], (0, 0))
        self.assertFalse(result['spherical'])

    def test_parse_near_spec_type_point(self):
        """GeoJSON with 'type' directly (not $geometry) success path (lines 649-656)."""
        result = parse_near_spec({'type': 'Point', 'coordinates': [0, 0]})
        self.assertEqual(result['query_point'], (0, 0))
        self.assertTrue(result['spherical'])

    def test_extract_near_specs_with_max_min_distance(self):
        spec = {
            'loc': {
                '$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}},
                '$maxDistance': 1000,
                '$minDistance': 100,
            }
        }
        near_specs, _cleaned = extract_near_specs(spec)
        self.assertEqual(len(near_specs), 1)
        _, parsed = near_specs[0]
        self.assertEqual(parsed['max_distance'], 1000)
        self.assertEqual(parsed['min_distance'], 100)

    def test_extract_near_specs_with_remaining(self):
        """Non-skip keys alongside $near populate cleaned (line 682)."""
        spec = {
            'loc': {
                '$near': {'$geometry': {'type': 'Point', 'coordinates': [0, 0]}},
                '$maxDistance': 1000,
                'otherField': 42,
            }
        }
        near_specs, cleaned = extract_near_specs(spec)
        self.assertEqual(len(near_specs), 1)
        self.assertEqual(cleaned, {'loc': {'otherField': 42}})

    def test_parse_near_spec_empty_dict(self):
        """Empty dict hits final fallback raise (line 657)."""
        with self.assertRaises(OperationFailure):
            parse_near_spec({})


class NearFilterTest(unittest.TestCase):
    """Group 3: near_filter unit tests (euclidean distance)."""

    def test_near_filter_euclidean_distance_hits(self):
        doc = {'type': 'Point', 'coordinates': [0, 0]}
        ok, dist = near_filter(doc, (1, 0), spherical=False)
        self.assertTrue(ok)
        self.assertAlmostEqual(dist, 1.0)

    def test_near_filter_euclidean_non_point_skipped(self):
        doc = {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]}
        ok, dist = near_filter(doc, (0, 0), spherical=False)
        self.assertFalse(ok)
        self.assertIsNone(dist)


class PointOnSegmentTest(unittest.TestCase):
    """Group 4: _point_on_segment edge cases."""

    def test_zero_length_segment(self):
        """Identical start/end points: point must match."""
        self.assertTrue(_point_on_segment(1, 1, 1, 1, 1, 1))

    def test_point_not_on_line(self):
        """Point above the segment (cross > epsilon)."""
        self.assertFalse(_point_on_segment(0, 1, 0, 0, 1, 0))

    def test_point_beyond_segment_start(self):
        """Point before segment start (dot < 0)."""
        self.assertFalse(_point_on_segment(-1, 0, 0, 0, 1, 0))


class SegmentsIntersectTest(unittest.TestCase):
    """Group 5: _segments_intersect collinear endpoint cases."""

    def test_o1_zero_within_bounds(self):
        """o1 == 0: p2 collinear with p1-q1 and within bounds (line 374)."""
        self.assertTrue(_segments_intersect((0, 0), (2, 0), (1, 0), (3, 0)))

    def test_o2_zero_shared_endpoint(self):
        """o2 == 0: q2 collinear with p1-q1 and within bounds."""
        self.assertTrue(_segments_intersect((0, 0), (2, 0), (4, 0), (1, 0)))

    def test_o3_zero_shared_endpoint(self):
        """o3 == 0: p1 collinear with p2-q2 and within bounds."""
        self.assertTrue(_segments_intersect((1, 0), (2, 0), (0, 0), (4, 0)))


class IntersectsBranchesTest(unittest.TestCase):
    """_point_intersects_geo branches (Group 6 supplement)."""

    def test_point_intersects_multipoint(self):
        """Point doc with MultiPoint query where one matches (line 289)."""
        doc = {'type': 'Point', 'coordinates': [5, 5]}
        query = {'type': 'MultiPoint', 'coordinates': [[5, 5], [20, 20]]}
        self.assertTrue(geo_intersects(doc, query))

    def test_point_intersects_multipoint_none(self):
        """Point doc with MultiPoint where none match."""
        doc = {'type': 'Point', 'coordinates': [1, 1]}
        query = {'type': 'MultiPoint', 'coordinates': [[5, 5], [20, 20]]}
        self.assertFalse(geo_intersects(doc, query))

    def test_point_intersects_multilinestring(self):
        """Point doc with MultiLineString query where one contains point."""
        doc = {'type': 'Point', 'coordinates': [5, 5]}
        query = {
            'type': 'MultiLineString',
            'coordinates': [[[0, 0], [10, 10]], [[20, 20], [30, 30]]],
        }
        self.assertTrue(geo_intersects(doc, query))

    def test_point_intersects_multilinestring_none(self):
        """Point doc with MultiLineString query where none contain point."""
        doc = {'type': 'Point', 'coordinates': [1, 2]}
        query = {
            'type': 'MultiLineString',
            'coordinates': [[[0, 0], [10, 10]], [[20, 20], [30, 30]]],
        }
        self.assertFalse(geo_intersects(doc, query))


class WithinBranchesTest(unittest.TestCase):
    """_point_within_geo branches (Group 8 supplement)."""

    def test_point_within_multipolygon(self):
        """Point doc with MultiPolygon query (line 302-303)."""
        doc = {'type': 'Point', 'coordinates': [5, 5]}
        query = {
            'type': 'MultiPolygon',
            'coordinates': [[[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]],
        }
        self.assertTrue(geo_within(doc, query))

    def test_point_within_point(self):
        """Point doc with Point query exact match (lines 304-306)."""
        doc = {'type': 'Point', 'coordinates': [5, 5]}
        query = {'type': 'Point', 'coordinates': [5, 5]}
        self.assertTrue(geo_within(doc, query))

    def test_point_within_point_mismatch(self):
        """Point doc with different Point query."""
        doc = {'type': 'Point', 'coordinates': [5, 5]}
        query = {'type': 'Point', 'coordinates': [1, 1]}
        self.assertFalse(geo_within(doc, query))

    def test_point_within_multipoint(self):
        """Point doc with MultiPoint query where one matches (lines 307-308)."""
        doc = {'type': 'Point', 'coordinates': [5, 5]}
        query = {'type': 'MultiPoint', 'coordinates': [[1, 1], [5, 5]]}
        self.assertTrue(geo_within(doc, query))

    def test_point_within_multipoint_none(self):
        """Point doc with MultiPoint query where none match."""
        doc = {'type': 'Point', 'coordinates': [5, 5]}
        query = {'type': 'MultiPoint', 'coordinates': [[1, 1], [2, 2]]}
        self.assertFalse(geo_within(doc, query))


class WithinMultiPointTest(unittest.TestCase):
    """geo_within MultiPoint doc branch (lines 253-254)."""

    def test_multipoint_all_within_polygon(self):
        """MultiPoint doc where all points are inside query polygon."""
        doc = {'type': 'MultiPoint', 'coordinates': [[1, 1], [9, 9]]}
        query = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        self.assertTrue(geo_within(doc, query))

    def test_multipoint_not_all_within_polygon(self):
        """MultiPoint doc where one point is outside query polygon."""
        doc = {'type': 'MultiPoint', 'coordinates': [[1, 1], [20, 20]]}
        query = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        self.assertFalse(geo_within(doc, query))


class LinestringWithinGeoTest(unittest.TestCase):
    """_linestring_within_geo branches (lines 515-516)."""

    def test_linestring_within_multipolygon(self):
        """LineString doc within one polygon of query MultiPolygon."""
        doc = {'type': 'LineString', 'coordinates': [[1, 1], [2, 2], [3, 3]]}
        query = {
            'type': 'MultiPolygon',
            'coordinates': [[[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]],
        }
        self.assertTrue(geo_within(doc, query))

    def test_linestring_not_within_multipolygon(self):
        """LineString doc not fully within any query polygon."""
        doc = {'type': 'LineString', 'coordinates': [[1, 1], [20, 20]]}
        query = {
            'type': 'MultiPolygon',
            'coordinates': [[[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]],
        }
        self.assertFalse(geo_within(doc, query))

    def test_linestring_within_linestring(self):
        """LineString doc with matching LineString query (lines 517-520)."""
        doc = {'type': 'LineString', 'coordinates': [[0, 0], [1, 1], [2, 2]]}
        query = {'type': 'LineString', 'coordinates': [[0, 0], [2, 2]]}
        self.assertTrue(geo_within(doc, query))

    def test_linestring_within_linestring_mismatch(self):
        """LineString doc with different endpoint LineString query (line 521)."""
        doc = {'type': 'LineString', 'coordinates': [[0, 0], [10, 10]]}
        query = {'type': 'LineString', 'coordinates': [[0, 0], [2, 2]]}
        self.assertFalse(geo_within(doc, query))


class GeoIntersectsPolygonQueryTest(unittest.TestCase):
    """Group 6: _polygon_intersects_geo query type branches."""

    def test_polygon_intersects_point(self):
        """Polygon doc with Point query inside."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        query = {'type': 'Point', 'coordinates': [5, 5]}
        self.assertTrue(geo_intersects(doc, query))

    def test_polygon_intersects_multipoint(self):
        """Polygon doc with MultiPoint query where one is inside."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        query = {'type': 'MultiPoint', 'coordinates': [[5, 5], [20, 20]]}
        self.assertTrue(geo_intersects(doc, query))

    def test_polygon_intersects_multipoint_none(self):
        """Polygon doc with MultiPoint query where none inside."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        query = {'type': 'MultiPoint', 'coordinates': [[20, 20], [30, 30]]}
        self.assertFalse(geo_intersects(doc, query))

    def test_polygon_intersects_linestring(self):
        """Polygon doc with LineString query crossing it."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        query = {'type': 'LineString', 'coordinates': [[5, -5], [5, 15]]}
        self.assertTrue(geo_intersects(doc, query))

    def test_polygon_intersects_linestring_disjoint(self):
        """Polygon doc with LineString query not touching."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        query = {'type': 'LineString', 'coordinates': [[20, 20], [30, 30]]}
        self.assertFalse(geo_intersects(doc, query))

    def test_polygon_intersects_multilinestring(self):
        """Polygon doc with MultiLineString query where one crosses."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        query = {
            'type': 'MultiLineString',
            'coordinates': [[[5, -5], [5, 15]], [[20, 20], [30, 30]]],
        }
        self.assertTrue(geo_intersects(doc, query))

    def test_polygon_edges_intersect_query(self):
        """Polygon doc where first vertex outside but edges overlap (line 544)."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[5, -5], [5, 5], [15, 5], [15, -5], [5, -5]]],
        }
        query = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        self.assertTrue(geo_intersects(doc, query))


class LinestringIntersectsGeoTest(unittest.TestCase):
    """Group 7: _linestring_intersects_geo branches."""

    def test_linestring_intersects_multipolygon(self):
        """LineString doc with MultiPolygon query."""
        doc = {'type': 'LineString', 'coordinates': [[5, -5], [5, 15]]}
        query = {
            'type': 'MultiPolygon',
            'coordinates': [[[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]],
        }
        self.assertTrue(geo_intersects(doc, query))

    def test_linestring_intersects_multipoint(self):
        """LineString doc with MultiPoint query where a point is on the line."""
        doc = {'type': 'LineString', 'coordinates': [[0, 0], [10, 10]]}
        query = {'type': 'MultiPoint', 'coordinates': [[5, 5], [20, 20]]}
        self.assertTrue(geo_intersects(doc, query))

    def test_linestring_intersects_multilinestring(self):
        """LineString doc with MultiLineString query where one crosses."""
        doc = {'type': 'LineString', 'coordinates': [[0, 0], [10, 10]]}
        query = {
            'type': 'MultiLineString',
            'coordinates': [[[0, 10], [10, 0]], [[20, 20], [30, 30]]],
        }
        self.assertTrue(geo_intersects(doc, query))


class PolygonWithinGeoTest(unittest.TestCase):
    """Group 8: _polygon_within_geo branches."""

    def test_polygon_not_within_polygon(self):
        """Doc polygon extends outside query polygon."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[-1, -1], [11, -1], [11, 11], [-1, 11], [-1, -1]]],
        }
        query = {
            'type': 'Polygon',
            'coordinates': [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        }
        self.assertFalse(geo_within(doc, query))

    def test_polygon_within_multipolygon(self):
        """Doc polygon within one polygon of query MultiPolygon."""
        doc = {
            'type': 'Polygon',
            'coordinates': [[[1, 1], [9, 1], [9, 9], [1, 9], [1, 1]]],
        }
        query = {
            'type': 'MultiPolygon',
            'coordinates': [[[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]],
        }
        self.assertTrue(geo_within(doc, query))


if __name__ == '__main__':
    unittest.main()
