# Geospatial Query Support

## Overview

mongomock-ng supports MongoDB geospatial query operators on GeoJSON data.
All operators work with `2dsphere`-style GeoJSON documents stored as
`{type: "<GeoJSON type>", coordinates: [...]}`.

## GeoJSON Format

Coordinate ordering follows the GeoJSON standard: **longitude first, latitude second**.

### Supported Geometry Types

| Type | Example |
|------|---------|
| `Point` | `{"type": "Point", "coordinates": [lon, lat]}` |
| `LineString` | `{"type": "LineString", "coordinates": [[lon1, lat1], [lon2, lat2], ...]}` |
| `Polygon` | `{"type": "Polygon", "coordinates": [[ring1], [ring2, ...]]}` |
| `MultiPoint` | `{"type": "MultiPoint", "coordinates": [[lon1, lat1], [lon2, lat2], ...]}` |
| `MultiLineString` | `{"type": "MultiLineString", "coordinates": [[line1], [line2], ...]}` |
| `MultiPolygon` | `{"type": "MultiPolygon", "coordinates": [[poly1], [poly2], ...]}` |
| `GeometryCollection` | `{"type": "GeometryCollection", "geometries": [...]}` |

### Validation Rules

- Longitude must be in [-180, 180]
- Latitude must be in [-90, 90]
- Polygon rings must be closed (first point == last point)
- Polygon rings must have at least 4 points

```python
from mongomock import MongoClient

client = MongoClient()
collection = client.db.places

# Insert a GeoJSON Point
collection.insert_one({
    "_id": 1,
    "name": "Home",
    "location": {"type": "Point", "coordinates": [-73.97, 40.77]}
})
```

## Query Operators

### `$geoIntersects`

Finds documents where the geometry intersects the query geometry.

```python
# Find documents that intersect a Polygon
results = collection.find({
    "location": {
        "$geoIntersects": {
            "$geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-74.0, 40.7],
                    [-73.9, 40.7],
                    [-73.9, 40.8],
                    [-74.0, 40.8],
                    [-74.0, 40.7]
                ]]
            }
        }
    }
})
```

Supports query geometries: `Polygon`, `MultiPolygon`, `Point`, `MultiPoint`,
`LineString`, `MultiLineString`.

### `$geoWithin`

Finds documents where the geometry is entirely within the query geometry.

```python
# Find documents inside a Polygon
results = collection.find({
    "location": {
        "$geoWithin": {
            "$geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-74.0, 40.7],
                    [-73.9, 40.7],
                    [-73.9, 40.8],
                    [-74.0, 40.8],
                    [-74.0, 40.7]
                ]]
            }
        }
    }
})
```

Supports query geometries: `Polygon`, `MultiPolygon`, `Point`, `MultiPoint`.

### `$near` / `$nearSphere`

Returns documents sorted by distance from a point.

```python
# Near a point (GeoJSON syntax)
results = collection.find(
    {"location": {"$near": {"$geometry": {"type": "Point", "coordinates": [-73.97, 40.77]}}}}
)

# With $maxDistance (in meters)
results = collection.find({
    "location": {
        "$near": {
            "$geometry": {"type": "Point", "coordinates": [-73.97, 40.77]},
            "$maxDistance": 1000
        }
    }
})

# With $minDistance (in meters)
results = collection.find({
    "location": {
        "$near": {
            "$geometry": {"type": "Point", "coordinates": [-73.97, 40.77]},
            "$minDistance": 500
        }
    }
})
```

Key behaviors:
- Results are sorted by distance ascending when no `sort()` is specified
- `$nearSphere` uses spherical (haversine) distance; `$near` uses planar (Euclidean) distance
- Documents without the location field or with non-Point geometry are skipped
- If `sort()` is explicitly called, distance-based sort is overridden

#### Legacy Array Syntax

```python
# $near with legacy coordinate pair [lon, lat]
results = collection.find({"location": {"$near": [-73.97, 40.77]}})
```

## Aggregation

### `$geoNear`

Returns documents sorted by distance from a point, adding a calculated distance field.

```python
pipeline = [
    {
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [-73.97, 40.77]},
            "distanceField": "distance",
            "spherical": True,
            "maxDistance": 2000,
            "minDistance": 100,
            "limit": 10,
            "query": {"category": "restaurant"},
            "key": "location"
        }
    }
]
results = list(collection.aggregate(pipeline))
```

#### Options

| Option | Type | Description |
|--------|------|-------------|
| `near` | GeoJSON Point or `[lon, lat]` | Reference point (required) |
| `distanceField` | string | Output field name for distance in meters (required) |
| `spherical` | bool | Use spherical distance (default: `False`; GeoJSON `near` forces `True`) |
| `maxDistance` | number | Maximum distance in meters |
| `minDistance` | number | Minimum distance in meters |
| `limit` | number | Maximum number of documents to return |
| `num` | number | Alias for `limit` |
| `query` | dict | Additional filter query |
| `key` | string | Field path to the location field (default: auto-detects `location`, `geo`, `coordinates`, `loc`) |
| `distanceMultiplier` | number | Multiplier applied to all distances (e.g., `0.001` for meters to kilometers) |
| `includeLocs` | string | Output field name for the location GeoJSON object used in distance calculation |

```python
pipeline = [
    {
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [-73.97, 40.77]},
            "distanceField": "dist_km",
            "spherical": True,
            "distanceMultiplier": 0.001,
            "includeLocs": "usedLocation",
        }
    }
]
results = list(collection.aggregate(pipeline))
# results[0] contains both "dist_km" (in km) and "usedLocation" (the GeoJSON point)
```

Key behaviors:
- Documents are sorted by distance ascending
- Documents with non-Point geometry or missing location are skipped
- If `near` is a GeoJSON object, `spherical` defaults to `True`

## Error Handling

```python
from mongomock import OperationFailure

# Missing $geometry field
try:
    collection.find({"loc": {"$geoIntersects": {}}})
except OperationFailure as e:
    print(e)  # "$geoIntersects requires a $geometry field"

# Invalid GeoJSON
try:
    collection.find({
        "loc": {
            "$geoIntersects": {
                "$geometry": {"type": "InvalidType", "coordinates": []}
            }
        }
    })
except OperationFailure as e:
    print(e)  # "Invalid GeoJSON type: 'InvalidType'. Must be one of ..."

# Missing required fields in $geoNear
try:
    collection.aggregate([{"$geoNear": {"near": {"type": "Point", "coordinates": [0, 0]}}}])
except OperationFailure as e:
    print(e)  # "Missing 'distanceField' in $geoNear"
```

## Limitations

- **2dsphere index**: `$near`, `$nearSphere`, and `$geoNear` **require** a
  `2dsphere` index on the queried field. Create one via
  `collection.create_index([("field_name", "2dsphere")])`. Without it, an
  `OperationFailure` is raised — matching real MongoDB behavior.
  `$geoIntersects` and `$geoWithin` do not have this requirement in the mock
  (though real MongoDB requires a 2dsphere index for all geospatial queries).
- **Custom CRS**: The `crs` field in `$geometry` is ignored. Only the default WGS 84 (EPSG:4326) coordinate reference system is supported.
- **Legacy operators**: `$center`, `$centerSphere`, `$box`, `$polygon` (for legacy coordinate pairs) are not supported. Use GeoJSON `$geometry` syntax.

## Testing

```python
def test_geospatial_query():
    client = MongoClient()
    collection = client.db.test

    # Required for $near/$nearSphere/$geoNear
    collection.create_index([("loc", "2dsphere")])

    collection.insert_many([
        {"_id": 1, "loc": {"type": "Point", "coordinates": [0, 0]}},
        {"_id": 2, "loc": {"type": "Point", "coordinates": [10, 10]}},
    ])

    polygon = {
        "$geoIntersects": {
            "$geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [5, 0], [5, 5], [0, 5], [0, 0]]],
            }
        }
    }

    results = list(collection.find({"loc": polygon}))
    assert len(results) == 1
    assert results[0]["_id"] == 1
```

## See Also

- [MongoDB Geospatial Query Operators](https://www.mongodb.com/docs/manual/reference/operator/query-geospatial/)
- [MongoDB `$geoNear` Aggregation](https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/)
- [GeoJSON Specification (RFC 7946)](https://tools.ietf.org/html/rfc7946)
