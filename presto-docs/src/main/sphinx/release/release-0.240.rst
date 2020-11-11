=============
Release 0.240
=============

.. warning::
    There is a bug in LambdaDefinitionExpression canonicalization introduced since 0.238. For more details, go to :issue:`15424`.

**Highlights**
==============
* Add ability to spill window functions to local disk when a worker is out of memory.
* Add support for inlining SQL functions at query planning time.
* Add support for limit pushdown through union.
* Add :func:`geometry_from_geojson` and :func:`geometry_as_geojson` to convert geometries from and to GeoJSON format.

**Details**
==============

General Changes
_______________
* Fix compiler error due to incorrect LambdaDefinitionExpression canonicalization.
* Fix compiler error in certain situations where sql functions with same lambda are used multiple times.
* Add ``IF EXISTS`` and ``IF NOT EXISTS`` syntax to ``ALTER TABLE``.
* Add ``query.max-scan-physical-bytes`` configuration and ``query_max_scan_physical_bytes`` session properties to limit total number of bytes read from storage during table scan. The default limit is 1PB.
* Add support for inlining SQL functions at query planning time. This feature is enabled by default, and can be disabled with the ``inline_sql_functions`` session property.
* Add :func:`geometry_from_geojson` and :func:`geometry_as_geojson` to convert geometries from and to GeoJSON format.
* Add support for pushdown of dereference expressions for querying nested data. This can be enabled with the ``pushdown_dereference_enabled`` session property or the ``experimental.pushdown-dereference-enabled`` configuration property.
* Use local private credentials (json key file) to refresh GCS access token. Usage : presto-cli --extra-credential hive.gcs.credentials.path="${PRIVATE_KEY_JSON_PATH}".
* Add ability to spill window functions to local disk when a worker is out of memory.
* Add support for limit pushdown through union.

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

Druid Changes
_____________
* Add support for union all operation with more than 1 druid source.
* Add support for filter on top of Aggregation.
* Fix unhandled HTTP response error for druid client.

Elasticserarch Changes
______________________
* Add support for IP data type.

Geospatial Changes
__________________
* Improve :func:`geometry_to_bing_tiles` performance.  It is 50x faster on complex polygons, the limit on polygon complexity is removed, and some correctness bugs have been fixed.
* Add geometry_to_dissolved_bing_tiles function, which dissolves complete sets of child tiles to their parent.
* Introduce :func:`bing_tile_children` and :func:`bing_tile_parent` functions to get parents and children of a Bing tile.

Hive Changes
____________
* Fix parquet statistics when min/max is not set.
* Improve split generation performance.
* Add support for Hudi realtime input format for hudi realtime queries.
* Add support for splitting hive files when skip.header.line.count=1.
* Allow presto-hive to use custom parquet input formats.

Kafka Changes
_____________
* Support ``INSERT`` in Kafka connector.

SPI Changes
___________
* Allow procedures to accept optional parameters.
