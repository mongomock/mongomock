# Aggregation Pipeline

## Supported stages

| Stage | Description |
|-------|-------------|
| `$match` | Filter documents (standard query operators) |
| `$group` | Group by _id, accumulators ($sum, $avg, $min, $max, $first, $last, $push, $addToSet) |
| `$sort` | Sort documents |
| `$project` | Include/exclude/reshape fields |
| `$limit` | Limit documents |
| `$skip` | Skip documents |
| `$unwind` | Deconstruct array field |
| `$lookup` | Left outer join (from, localField, foreignField, as; also pipeline syntax) |
| `$addFields` | Add new fields |
| `$set` | Alias for $addFields |
| `$unset` | Remove fields |
| `$count` | Count documents |
| `$replaceRoot` | Replace document root |
| `$replaceWith` | Alias for $replaceRoot |
| `$bucket` | Bucket documents into groups |
| `$bucketAuto` | Auto-bucket documents |
| `$facet` | Multiple pipelines in one stage |
| `$sortByCount` | Sort by count of a field |
| `$out` | Write to collection |
| `$merge` | Merge into collection |
| `$geoNear` | Geospatial aggregation (requires 2dsphere index) |
| `$sample` | Random sample |
| `$setWindowFields` | Window computation (partial support) |
| `$fill` | Fill missing values (partial support) |
| `$densify` | Create missing documents |
| `$unionWith` | Union with another collection |
| `$graphLookup` | Graph traversal (basic support) |
| `$match` and `$expr` | Use aggregation expressions in query |

## Supported operators (in $project, $addFields, etc.)

### Arithmetic

`$abs`, `$add`, `$ceil`, `$divide`, `$exp`, `$floor`, `$ln`, `$log`, `$log10`, `$mod`, `$multiply`, `$pow`, `$round`, `$sqrt`, `$subtract`, `$trunc`

### Array

`$arrayElemAt`, `$concatArrays`, `$filter`, `$first`, `$in`, `$indexOfArray`, `$isArray`, `$last`, `$map`, `$range`, `$reduce`, `$reverseArray`, `$size`, `$slice`, `$zip`

### Boolean

`$and`, `$or`, `$not`

### Comparison

`$cmp`, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`

### Conditional

`$cond`, `$ifNull`, `$switch`

### Date

`$dateFromString`, `$dateFromParts`, `$dateToParts`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$hour`, `$millisecond`, `$minute`, `$month`, `$second`, `$week`, `$year`, `$isoDayOfWeek`, `$isoWeek`, `$isoWeekYear`, `$dateToString`

### Object

`$mergeObjects`, `$objectToArray`

### Set

`$allElementsTrue`, `$anyElementTrue`, `$setDifference`, `$setEquals`, `$setIntersection`, `$setIsSubset`, `$setUnion`

### String

`$concat`, `$indexOfBytes`, `$ltrim`, `$regexMatch`, `$replaceAll`, `$replaceOne`, `$rtrim`, `$split`, `$strLenBytes`, `$strcasecmp`, `$substr`, `$substrBytes`, `$substrCP`, `$toLower`, `$toString`, `$trim`, `$toUpper`

### Type

`$convert`, `$toBool`, `$toDate`, `$toDecimal`, `$toDouble`, `$toInt`, `$toLong`, `$toString`, `$type`, `$isNumber`

### Accumulators (in $group)

`$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`, `$stdDevPop`, `$stdDevSamp`

## Current version

mongomock-ng v7.7.0 — implements most common operators. Check docs/LIMITATIONS.md for known gaps.
