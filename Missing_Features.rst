What is this?
-------------
This document lists down the features missing in mongomock library. PRs for these features are highly appreciated.

If i miss to include a feature in the below list, Please feel free to add to the below list and raise a PR.

* $rename complex operations - https://docs.mongodb.com/manual/reference/operator/update/rename/
* create_collection options - https://docs.mongodb.com/v3.2/reference/method/db.createCollection/#definition
* bypass_document_validation options
* session options
* Operators in the aggregate pipeline:

  * `$addFields <https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/>`_
  * `$bucket <https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/>`_
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
  * `$multiply <https://docs.mongodb.com/manual/reference/operator/aggregation/multiply/>`_
  * `$replaceRoot <https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/>`_
  * `$sortByCount <https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/>`_
