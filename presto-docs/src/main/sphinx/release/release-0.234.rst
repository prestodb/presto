=============
Release 0.234
=============

General Changes
_______________
* Fix query failure for cross-joining bucketed tables with ``COALESCE`` when the bucket counts are compatible but mismatched.
* Fix an issue where ``IGNORE NULLS`` is not respected for window functions.
* Fix scheduling regression by setting the default scheduler to legacy scheduler.
* Improve coordinator reliability by adding support to cap the number of total tasks running in a coordinator by pausing scheduling.
  This can be enabled by the configuration property ``experimental.max-total-running-task-count-to-not-execute-new-query``.
* Improve the scale writer heuristics by considering overall producer buffer utilization. This can be enabled by the session property
  ``optimized_scale_writer_producer_buffer`` and the configuration property ``optimized-scale-writer-producer-buffer``.
* Improve end point ``v1/resourceGroupState`` to supporting filtering of resource groups that are dynamically generated.
* Improve connection pooling to avoid running out of sockets.
* Add :ref:`khyperloglog_type` type and related functions.
* Add support for forcing streaming exchange for Mark Distinct even if materialized exchange is enabled.
  This can be enabled by the session property ``use_stream_exchange_for_mark_distinct``
  and the configuration property ``query.use-streaming-exchange-for-mark-distinct``. (:pr:`14216`).

Geospatial Changes
__________________
* Improve storage efficiency for Bing tiles by storing Bing tiles as ``BIGINT``. This also reduces bucket skew in certain cases.
* Add support for spatial joins for join condition ``ST_Distance(p1, p2) < r``.

Hive Changes
____________
* Add ``ZSTD`` support for writing ``ORC`` and ``DWRF`` files. This can be enabled by setting session property ``hive.compression_codec`` to ``ZSTD``.

Verifier Changes
________________
* Add support for verifying ``SELECT`` queries that produce ``DATE`` or ``UNKNOWN`` (null) columns, or structured typed columns with ``DATE`` or ``UNKNOWN``.
* Add support for auto-resolving control check query failures due to ``EXCEEDED_TIME_LIMIT``.
* Add determinism analysis support for simple queries with top-level ``ORDER BY LIMIT`` clause. (:pr:`14181`).

SPI Changes
___________
* Add parameter ``AccessControlContext`` to all methods in ``SystemAccessControl``.
* Add ``firstDynamicSegmentPosition`` to SelectionContext.

Druid Changes
_____________
* Add support for aggregation pushdown.
* Add support for ``LIMIT`` evaluation pushdown.
