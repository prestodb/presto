=============
Release 0.240
=============

General Changes
_______________
* Fix LambdaDefinitionExpression canonicalization did not handle CAST.
* Fix compiler error in certain situations where sql functions with same lambda are used multiple times.
* Add `IF EXISTS `and `IF NOT EXISTS` syntax to `ALTER TABLE`.
* Add `query.max-scan-physical-bytes` configuration and `query_max_scan_physical_bytes` session properties to limit total number of bytes read from storage during table scan. The default limit is 1PB.
* Add support for inlining SQL functions at query planning time. This feature is enabled by default, and can be disabled with the ``inline_sql_functions`` session property.
* Added :func:`geometry_from_geojson` and :func:`geometry_as_geojson` to convert geometries from and to GeoJSON format.
* Allow procedures to have optional arguments with default values.
* Push down dereference expression.
* Use local private credentials (json key file) to refresh GCS access token presto-cli --extra-credential hive.gcs.credentials.path="${PRIVATE_KEY_JSON_PATH}".

Thrift Connector Changes
________________________
* Rename ``presto-thrift-connector-api`` to ``presto-thrift-api`` and have separate packages for datatypes, valuesets and connector.

Verifier Changes
________________
* Fix an issue where Verifier fails to start when failure resolver is disabled.
* Add configuration property ``test_name``, to be passed in to the client info blob.
* Add support to implement customized way of launching Presto queries.
* Add support to populate client info for the queries issued by Verifier.
* Add support to resubmit verification if test query fails with ``HIVE_PARTITION_OFFLINE``.
* Add support to run helper queries on a separate cluster other than the control cluster.
* Add support to skip running control queries and comparing results. This can be enabled by configuration property ``skip-control``.

Cassandra Changes
_________________
* Add TLS security support.

Geospatial Changes
__________________
* Improve geometry_to_bing_tiles.  It is 50x faster on complex polygons, the limit on polygon complexity is removed, and some correctness bugs have been fixed.
* Add geometry_to_dissolved_bing_tiles function, which dissolves complete sets of child tiles to their parent.
* Introduce ``bing_tile_children`` and ``bing_tile_parent`` functions to get parents and children of a Bing tile.

Hive Changes
____________
* Fix parquet statistics when min/max is not set.
* Improves split generation by avoiding an unncessary splittable check when files are smaller than the initial split max size, regardless of their input format.
* Add support for Hudi realtime input format for hudi realtime queries.
* Adds support for splitting hive files when skip.header.line.count=1.
* Allows presto-hive to use custom parquet input formats.

Kafka Changes
_____________
* Support insert in Kafka connector.
