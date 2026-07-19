from __future__ import annotations

import itertools
import math
from collections.abc import Sequence
from typing import Any

from . import OperationFailure


GEOJSON_TYPES = frozenset(
    {
        'Point',
        'LineString',
        'Polygon',
        'MultiPoint',
        'MultiLineString',
        'MultiPolygon',
        'GeometryCollection',
    }
)


def parse_geojson(obj: Any) -> dict:
    if not isinstance(obj, dict):
        raise OperationFailure('GeoJSON must be an object')
    typ = obj.get('type')
    if not isinstance(typ, str) or typ not in GEOJSON_TYPES:
        raise OperationFailure(
            f'Invalid GeoJSON type: {typ!r}. Must be one of {", ".join(sorted(GEOJSON_TYPES))}'
        )
    coordinates = obj.get('coordinates')
    if typ == 'GeometryCollection':
        geometries = obj.get('geometries', [])
        if not isinstance(geometries, list):
            raise OperationFailure('GeometryCollection must have an array of geometries')
        return {'type': typ, 'geometries': [parse_geojson(g) for g in geometries]}
    _validate_coordinates(typ, coordinates)
    return {'type': typ, 'coordinates': coordinates}


def _validate_coordinates(typ: str, coords: Any) -> None:
    if not isinstance(coords, (list, tuple)):
        raise OperationFailure(f'GeoJSON {typ} coordinates must be an array')
    expected_depth = _coord_depth(typ)
    actual_depth = _depth(coords)
    if actual_depth != expected_depth:
        raise OperationFailure(
            f'GeoJSON {typ} coordinates must be an array of depth {expected_depth}'
        )


def _coord_depth(typ: str) -> int:
    if typ == 'Point':
        return 1
    if typ in ('LineString', 'MultiPoint'):
        return 2
    if typ in ('Polygon', 'MultiLineString'):
        return 3
    if typ == 'MultiPolygon':
        return 4
    return 1


def _depth(val: Any, d: int = 0) -> int:
    if isinstance(val, (list, tuple)):
        if not val:
            return d + 1
        return _depth(val[0], d + 1)
    return d


def validate_coord_range(lon: float, lat: float) -> None:
    if not isinstance(lon, (int, float)):
        raise OperationFailure(f'longitude must be a number, got {type(lon).__name__}')
    if not isinstance(lat, (int, float)):
        raise OperationFailure(f'latitude must be a number, got {type(lat).__name__}')
    if lon < -180 or lon > 180:
        raise OperationFailure(
            f'Coordinate {lon} is not valid for longitude. Must be in [-180, 180]'
        )
    if lat < -90 or lat > 90:
        raise OperationFailure(f'Coordinate {lat} is not valid for latitude. Must be in [-90, 90]')


def validate_geojson(obj: dict) -> None:
    typ = obj['type']
    if typ == 'GeometryCollection':
        for geom in obj.get('geometries', []):
            validate_geojson(geom)
        return
    coords = obj['coordinates']
    if typ == 'Point':
        _validate_point_coords(coords)
    elif typ == 'LineString':
        _validate_linestring_coords(coords)
    elif typ == 'Polygon':
        _validate_polygon_coords(coords)
    elif typ == 'MultiPoint':
        for pt in coords:
            _validate_point_coords(pt)
    elif typ == 'MultiLineString':
        for ls in coords:
            _validate_linestring_coords(ls)
    elif typ == 'MultiPolygon':
        for poly in coords:
            _validate_polygon_coords(poly)


def _validate_point_coords(coords: Sequence) -> None:
    if len(coords) < 2:
        raise OperationFailure('Point must have at least 2 coordinates')
    lon, lat = float(coords[0]), float(coords[1])
    validate_coord_range(lon, lat)


def _validate_linestring_coords(coords: Sequence) -> None:
    if len(coords) < 2:
        raise OperationFailure('LineString must have at least 2 points')
    for pt in coords:
        _validate_point_coords(pt)


def _validate_polygon_coords(coords: Sequence) -> None:
    if not coords:
        raise OperationFailure('Polygon must have at least 1 ring')
    for ring in coords:
        if len(ring) < 4:
            raise OperationFailure('Polygon ring must have at least 4 points (first = last)')
        if not _points_equal(tuple(ring[0][:2]), tuple(ring[-1][:2])):
            raise OperationFailure('Polygon ring is not closed')
        for pt in ring:
            _validate_point_coords(pt)


def point_from_geojson(obj: dict) -> tuple[float, float]:
    return tuple(obj['coordinates'][:2])


def point_in_polygon_ray_casting(
    point: tuple[float, float],
    ring: list[tuple[float, float]],
) -> bool:
    x, y = point
    inside = False
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]

        if y1 == y2 and y == y1:
            min_x = min(x1, x2)
            max_x = max(x1, x2)
            if min_x <= x <= max_x:
                return True
        elif (y1 > y) != (y2 > y):
            if x2 == x1:
                if x1 > x:
                    inside = not inside
            else:
                xinters = x1 + (x - x1) * (y2 - y1) / (x2 - x1)
                if abs(xinters - x) < 1e-12:
                    return True
                if xinters > x:
                    inside = not inside
    return inside


def point_in_polygon(
    point: tuple[float, float],
    coordinates: list,
) -> bool:
    outer = _to_float_pairs(coordinates[0])
    if not point_in_polygon_ray_casting(point, outer):
        return False
    for ring in coordinates[1:]:
        inner = _to_float_pairs(ring)
        if point_in_polygon_ray_casting(point, inner):
            return False
    return True


def point_in_multipolygon(
    point: tuple[float, float],
    coordinates: list,
) -> bool:
    return any(point_in_polygon(point, polygon_coords) for polygon_coords in coordinates)


def _to_float_pairs(ring: Sequence) -> list[tuple[float, float]]:
    return [(float(p[0]), float(p[1])) for p in ring]


def haversine_distance(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
) -> float:
    earth_radius = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius * c


def geo_intersects(doc_geo: dict, query_geo: dict) -> bool:
    doc_type = doc_geo['type']

    if doc_type == 'Point':
        pt = point_from_geojson(doc_geo)
        return _point_intersects_geo(pt, query_geo)

    if doc_type == 'MultiPoint':
        pts = [tuple(p[:2]) for p in doc_geo['coordinates']]
        return any(_point_intersects_geo(pt, query_geo) for pt in pts)

    if doc_type == 'LineString':
        return _linestring_intersects_geo(_to_float_pairs(doc_geo['coordinates']), query_geo)

    if doc_type == 'MultiLineString':
        return any(
            _linestring_intersects_geo(_to_float_pairs(ls), query_geo)
            for ls in doc_geo['coordinates']
        )

    if doc_type == 'Polygon':
        return _polygon_intersects_geo(doc_geo['coordinates'], query_geo)

    if doc_type == 'MultiPolygon':
        return any(
            _polygon_intersects_geo(poly_coords, query_geo)
            for poly_coords in doc_geo['coordinates']
        )

    if doc_type == 'GeometryCollection':
        return any(geo_intersects(g, query_geo) for g in doc_geo.get('geometries', []))

    return False


def geo_within(doc_geo: dict, query_geo: dict) -> bool:
    doc_type = doc_geo['type']

    if doc_type == 'Point':
        pt = point_from_geojson(doc_geo)
        return _point_within_geo(pt, query_geo)

    if doc_type == 'MultiPoint':
        pts = [tuple(p[:2]) for p in doc_geo['coordinates']]
        return all(_point_within_geo(pt, query_geo) for pt in pts)

    if doc_type == 'LineString':
        return _linestring_within_geo(_to_float_pairs(doc_geo['coordinates']), query_geo)

    if doc_type == 'MultiLineString':
        return all(
            _linestring_within_geo(_to_float_pairs(ls), query_geo) for ls in doc_geo['coordinates']
        )

    if doc_type == 'Polygon':
        return _polygon_within_geo(doc_geo['coordinates'], query_geo)

    if doc_type == 'MultiPolygon':
        return all(
            _polygon_within_geo(poly_coords, query_geo) for poly_coords in doc_geo['coordinates']
        )

    if doc_type == 'GeometryCollection':
        return all(geo_within(g, query_geo) for g in doc_geo.get('geometries', []))

    return False


def _point_intersects_geo(pt: tuple[float, float], geo: dict) -> bool:
    typ = geo['type']
    coords = geo['coordinates']
    if typ == 'Polygon':
        return point_in_polygon(pt, coords)
    if typ == 'MultiPolygon':
        return point_in_multipolygon(pt, coords)
    if typ == 'Point':
        other = point_from_geojson(geo)
        return _points_equal(pt, other)
    if typ == 'MultiPoint':
        return any(_points_equal(pt, tuple(p[:2])) for p in coords)
    if typ == 'LineString':
        return _point_on_linestring(pt, _to_float_pairs(coords))
    if typ == 'MultiLineString':
        return any(_point_on_linestring(pt, _to_float_pairs(ls)) for ls in coords)
    return False


def _point_within_geo(pt: tuple[float, float], geo: dict) -> bool:
    typ = geo['type']
    coords = geo['coordinates']
    if typ == 'Polygon':
        return point_in_polygon(pt, coords)
    if typ == 'MultiPolygon':
        return point_in_multipolygon(pt, coords)
    if typ == 'Point':
        other = point_from_geojson(geo)
        return _points_equal(pt, other)
    if typ == 'MultiPoint':
        return any(_points_equal(pt, tuple(p[:2])) for p in coords)
    return False


def _points_equal(a: tuple, b: tuple) -> bool:
    return abs(a[0] - b[0]) < 1e-12 and abs(a[1] - b[1]) < 1e-12


def _point_on_linestring(
    pt: tuple[float, float],
    points: list[tuple[float, float]],
) -> bool:
    x, y = pt
    for i in range(len(points) - 1):
        x1, y1 = points[i]
        x2, y2 = points[i + 1]
        if _point_on_segment(x, y, x1, y1, x2, y2):
            return True
    return False


def _point_on_segment(
    px: float,
    py: float,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
) -> bool:
    dx = x2 - x1
    dy = y2 - y1
    if dx == 0 and dy == 0:
        return abs(px - x1) < 1e-12 and abs(py - y1) < 1e-12
    cross = (px - x1) * dy - (py - y1) * dx
    if abs(cross) > 1e-12:
        return False
    dot = (px - x1) * dx + (py - y1) * dy
    if dot < -1e-12:
        return False
    len_sq = dx * dx + dy * dy
    return not dot > len_sq + 1e-12


def _orientation(p: tuple, q: tuple, r: tuple) -> int:
    val = (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])
    if abs(val) < 1e-12:
        return 0
    return 1 if val > 0 else 2


def _on_segment_bounds(p: tuple, q: tuple, r: tuple) -> bool:
    return (
        min(p[0], q[0]) - 1e-12 <= r[0] <= max(p[0], q[0]) + 1e-12
        and min(p[1], q[1]) - 1e-12 <= r[1] <= max(p[1], q[1]) + 1e-12
    )


def _segments_intersect(p1: tuple, q1: tuple, p2: tuple, q2: tuple) -> bool:
    o1 = _orientation(p1, q1, p2)
    o2 = _orientation(p1, q1, q2)
    o3 = _orientation(p2, q2, p1)
    o4 = _orientation(p2, q2, q1)

    if o1 != o2 and o3 != o4:
        return True
    if o1 == 0 and _on_segment_bounds(p1, q1, p2):
        return True
    if o2 == 0 and _on_segment_bounds(p1, q1, q2):
        return True
    if o3 == 0 and _on_segment_bounds(p2, q2, p1):
        return True
    return bool(o4 == 0 and _on_segment_bounds(p2, q2, q1))


def _linestring_intersects_polygon(
    points: list[tuple[float, float]],
    coords: list,
) -> bool:
    if point_in_polygon(points[0], coords):
        return True

    poly_edges = _polygon_edges(coords)
    for i in range(len(points) - 1):
        a, b = points[i], points[i + 1]
        for pe in poly_edges:
            if _segments_intersect(a, b, pe[0], pe[1]):
                return True
    return False


def _polygon_edges(coords: list) -> list[tuple[tuple, tuple]]:
    edges = []
    for ring in coords:
        pts = _to_float_pairs(ring)
        for i in range(len(pts) - 1):
            edges.append((pts[i], pts[i + 1]))
    return edges


def _linestring_intersects_geo(
    points: list[tuple[float, float]],
    geo: dict,
) -> bool:
    typ = geo['type']
    coords = geo['coordinates']

    if typ == 'Polygon':
        return _linestring_intersects_polygon(points, coords)
    if typ == 'MultiPolygon':
        return any(_linestring_intersects_polygon(points, poly_coords) for poly_coords in coords)
    if typ == 'Point':
        other = point_from_geojson(geo)
        for a, b in itertools.pairwise(points):
            if _point_on_segment(other[0], other[1], a[0], a[1], b[0], b[1]):
                return True
        return _points_equal(points[0], other)
    if typ == 'MultiPoint':
        return any(_point_on_linestring(tuple(p[:2]), points) for p in coords)
    if typ == 'LineString':
        other_pts = _to_float_pairs(coords)
        return any(
            _segments_intersect(a, b, c, d)
            for a, b in itertools.pairwise(points)
            for c, d in itertools.pairwise(other_pts)
        )
    if typ == 'MultiLineString':
        return any(
            _linestring_intersects_geo(points, {'type': 'LineString', 'coordinates': ls})
            for ls in coords
        )
    return False


def _segments_cross(p1: tuple, q1: tuple, p2: tuple, q2: tuple) -> bool:
    o1 = _orientation(p1, q1, p2)
    o2 = _orientation(p1, q1, q2)
    o3 = _orientation(p2, q2, p1)
    o4 = _orientation(p2, q2, q1)

    if o1 == 0 and o2 == 0 and o3 == 0 and o4 == 0:
        return _collinear_segments_overlap(p1, q1, p2, q2)
    return o1 != o2 and o3 != o4


def _segments_cross_interior(p1: tuple, q1: tuple, p2: tuple, q2: tuple) -> bool:
    o1 = _orientation(p1, q1, p2)
    o2 = _orientation(p1, q1, q2)
    o3 = _orientation(p2, q2, p1)
    o4 = _orientation(p2, q2, q1)

    if o1 == 0 and o2 == 0 and o3 == 0 and o4 == 0:
        return _collinear_overlap_interior(p1, q1, p2, q2)
    if o1 == 0 or o2 == 0 or o3 == 0 or o4 == 0:
        return False
    return o1 != o2 and o3 != o4


def _collinear_segments_overlap(p1: tuple, q1: tuple, p2: tuple, q2: tuple) -> bool:
    def between(a: float, b: float, c: float) -> bool:
        return min(a, b) - 1e-12 <= c <= max(a, b) + 1e-12

    return (between(p1[0], q1[0], p2[0]) and between(p1[1], q1[1], p2[1])) or (
        between(p1[0], q1[0], q2[0]) and between(p1[1], q1[1], q2[1])
    )


def _collinear_overlap_interior(p1: tuple, q1: tuple, p2: tuple, q2: tuple) -> bool:
    def between_strict(a: float, b: float, c: float) -> bool:
        return min(a, b) + 1e-12 <= c <= max(a, b) - 1e-12

    def between_loose(a, b, c):
        return min(a, b) - 1e-12 <= c <= max(a, b) + 1e-12

    p2_on_q1 = between_strict(p1[0], q1[0], p2[0]) and between_strict(p1[1], q1[1], p2[1])
    q2_on_q1 = between_strict(p1[0], q1[0], q2[0]) and between_strict(p1[1], q1[1], q2[1])
    if p2_on_q1 or q2_on_q1:
        return True
    p1_on_q2 = between_loose(p2[0], q2[0], p1[0]) and between_loose(p2[1], q2[1], p1[1])
    q1_on_q2 = between_loose(p2[0], q2[0], q1[0]) and between_loose(p2[1], q2[1], q1[1])
    return (p1_on_q2 and q1_on_q2) and (not _points_equal(p1, p2) or not _points_equal(q1, q2))


def _linestring_within_polygon(
    points: list[tuple[float, float]],
    coords: list,
) -> bool:
    for pt in points:
        if not point_in_polygon(pt, coords):
            return False
    poly_edges = _polygon_edges(coords)
    for i in range(len(points) - 1):
        a, b = points[i], points[i + 1]
        for pe in poly_edges:
            if _segments_cross_interior(a, b, pe[0], pe[1]):
                return False
    return True


def _linestring_within_geo(
    points: list[tuple[float, float]],
    geo: dict,
) -> bool:
    typ = geo['type']
    coords = geo['coordinates']

    if typ == 'Polygon':
        return _linestring_within_polygon(points, coords)
    if typ == 'MultiPolygon':
        return any(_linestring_within_polygon(points, poly_coords) for poly_coords in coords)
    if typ == 'LineString':
        other_pts = _to_float_pairs(coords)
        if _points_equal(points[0], other_pts[0]) and _points_equal(points[-1], other_pts[-1]):
            return all(_point_on_linestring(pt, other_pts) for pt in points)
        return False
    return False


def _polygon_intersects_geo(coords: list, geo: dict) -> bool:
    typ = geo['type']
    query_coords = geo['coordinates']

    if typ in ('Polygon', 'MultiPolygon'):
        other_rings = [query_coords] if typ == 'Polygon' else query_coords

        doc_ring = _to_float_pairs(coords[0])
        if point_in_polygon(doc_ring[0], other_rings[0]):
            return True

        doc_edges = _polygon_edges(coords)
        other_edges_list = []
        for other_coords in other_rings:
            other_edges_list.extend(_polygon_edges(other_coords))

        for de in doc_edges:
            for oe in other_edges_list:
                if _segments_intersect(de[0], de[1], oe[0], oe[1]):
                    return True
        return False

    if typ in ('LineString', 'MultiLineString'):
        lines = query_coords if typ == 'MultiLineString' else [query_coords]
        doc_edges = _polygon_edges(coords)
        for ls in lines:
            pts = _to_float_pairs(ls)
            for i in range(len(pts) - 1):
                for de in doc_edges:
                    if _segments_intersect(de[0], de[1], pts[i], pts[i + 1]):
                        return True
        return False

    if typ == 'Point':
        pt = point_from_geojson(geo)
        return point_in_polygon(pt, coords)

    if typ == 'MultiPoint':
        return any(point_in_polygon(tuple(p[:2]), coords) for p in query_coords)

    return False


def _polygon_within_geo(coords: list, geo: dict) -> bool:
    typ = geo['type']
    query_coords = geo['coordinates']

    if typ == 'Polygon':
        doc_ring = _to_float_pairs(coords[0])
        for pt in doc_ring:
            if not point_in_polygon(pt, query_coords):
                return False
        doc_edges = _polygon_edges(coords)
        other_edges = _polygon_edges(query_coords)
        for de in doc_edges:
            for oe in other_edges:
                if _segments_cross_interior(de[0], de[1], oe[0], oe[1]):
                    return False
        return True

    if typ == 'MultiPolygon':
        return any(
            _polygon_within_geo(coords, {'type': 'Polygon', 'coordinates': poly_coords})
            for poly_coords in query_coords
        )

    return False


def near_filter(
    doc_geo: dict,
    query_point: tuple[float, float],
    max_distance: float | None = None,
    min_distance: float | None = None,
    spherical: bool = False,
) -> tuple[bool, float | None]:
    if doc_geo['type'] != 'Point':
        return False, None
    pt = point_from_geojson(doc_geo)
    if spherical:
        dist = haversine_distance(pt[0], pt[1], query_point[0], query_point[1])
    else:
        dist = _euclidean_distance(pt[0], pt[1], query_point[0], query_point[1])
    if min_distance is not None and dist < min_distance - 1e-12:
        return False, dist
    if max_distance is not None and dist > max_distance + 1e-12:
        return False, dist
    return True, dist


def _euclidean_distance(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    return math.sqrt((lon2 - lon1) ** 2 + (lat2 - lat1) ** 2)


def parse_near_spec(spec: Any) -> dict:
    if isinstance(spec, (list, tuple)):
        if len(spec) < 2:
            raise OperationFailure('$near requires a point with at least 2 coordinates')
        lon, lat = float(spec[0]), float(spec[1])
        validate_coord_range(lon, lat)
        return {
            'query_point': (lon, lat),
            'max_distance': None,
            'min_distance': None,
            'spherical': False,
        }
    if not isinstance(spec, dict):
        raise OperationFailure('$near spec must be an object or array')
    if '$geometry' in spec:
        geometry = parse_geojson(spec['$geometry'])
        if geometry['type'] != 'Point':
            raise OperationFailure('$geometry must be a Point')
        validate_geojson(geometry)
        query_point = point_from_geojson(geometry)
        return {
            'query_point': query_point,
            'max_distance': spec.get('$maxDistance'),
            'min_distance': spec.get('$minDistance'),
            'spherical': spec.get('$spherical', True),
        }
    if 'type' in spec:
        geometry = parse_geojson(spec)
        if geometry['type'] != 'Point':
            raise OperationFailure('$geometry must be a Point')
        validate_geojson(geometry)
        query_point = point_from_geojson(geometry)
        return {
            'query_point': query_point,
            'max_distance': spec.get('$maxDistance'),
            'min_distance': spec.get('$minDistance'),
            'spherical': spec.get('$spherical', True),
        }
    raise OperationFailure('$near requires a GeoJSON point or legacy coordinate pair')


def extract_near_specs(spec: dict) -> tuple[list[tuple[str, dict]], dict]:
    cleaned = {}
    near_specs: list[tuple[str, dict]] = []
    skip_keys = ('$near', '$nearSphere', '$maxDistance', '$minDistance')
    for key, val in spec.items():
        if isinstance(val, dict):
            if '$near' in val or '$nearSphere' in val:
                near_field = key
                near_op = '$nearSphere' if '$nearSphere' in val else '$near'
                is_spherical = near_op == '$nearSphere'
                near_val = val[near_op]
                parsed = parse_near_spec(near_val)
                parsed['spherical'] = parsed['spherical'] or is_spherical
                max_dist = val.get('$maxDistance')
                min_dist = val.get('$minDistance')
                if max_dist is not None:
                    parsed['max_distance'] = max_dist
                if min_dist is not None:
                    parsed['min_distance'] = min_dist
                near_specs.append((near_field, parsed))
                remaining = {k: v for k, v in val.items() if k not in skip_keys}
                if remaining:
                    cleaned[key] = remaining
            else:
                sub_near, sub_cleaned = extract_near_specs(val)
                near_specs.extend(sub_near)
                if sub_cleaned:
                    cleaned[key] = sub_cleaned
        else:
            cleaned[key] = val
    return near_specs, cleaned
