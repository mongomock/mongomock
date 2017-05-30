What is this?
-------------
This document lists down the features missing in mongomock library. PRs for these features are highly appreciated.

If i miss to include a feature in the below list, Please feel free to add to the below list and raise a PR.

* $rename complex operations - https://docs.mongodb.com/manual/reference/operator/update/rename/
* create_collection options - https://docs.mongodb.com/v3.2/reference/method/db.createCollection/#definition
* bypass_document_validation options
* session options
* `$slice modifier <https://docs.mongodb.com/manual/reference/operator/update/slice/>`_ during a $push operation
* Operations of the aggregate pipeline:

  * `$addFields <https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/>`_
  * `$bucketAuto <https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/>`_
  * `$collStats <https://docs.mongodb.com/manual/reference/operator/aggregation/collStats/>`_
  * `$count <https://docs.mongodb.com/manual/reference/operator/aggregation/count/>`_
  * `$currentOp <https://docs.mongodb.com/manual/reference/operator/aggregation/currentOp/>`_
  * `$facet <https://docs.mongodb.com/manual/reference/operator/aggregation/facet/>`_
  * `$geoNear <https://docs.mongodb.com/manual/reference/operator/aggregation/geoNear/>`_
  * `$graphLookup <https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup>`_
  * `$indexStats <https://docs.mongodb.com/manual/reference/operator/aggregation/indexStats/>`_
  * `$listLocalSessions <https://docs.mongodb.com/manual/reference/operator/aggregation/listLocalSessions/>`_
  * `$listSessions <https://docs.mongodb.com/manual/reference/operator/aggregation/listSessions/>`_
  * `$replaceRoot <https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/>`_
  * `$sortByCount <https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/>`_
* Operators within the aggregate pipeline:
  * Some arithmetic operators:
    * `$add <https://docs.mongodb.com/manual/reference/operator/aggregation/add/>`_
    * `$multiply <https://docs.mongodb.com/manual/reference/operator/aggregation/multiply/>`_
    * `$trunc <https://docs.mongodb.com/manual/reference/operator/aggregation/trunc/>`_
  * Boolean operators ($and, $or, $not)
  * Set operators ($setEquals, $setIntersection, $setDifference, …)
  * String operators ($concat, $strcasecmp, $substr, …)
  * Text search operator ($meta)
  * Projection operators ($map, $let)
  * Array operators ($concatArrays, $filter, $isArray, $slice)
