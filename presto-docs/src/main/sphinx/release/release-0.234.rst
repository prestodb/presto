=============
Release 0.234
=============

General Changes
_______________
* Enable spatial joins for `ST_Distance(p1, p2) < r` for spherical geography points `p1` and `p2`.
* Fix query failures with incompatible partition handle when session property ``partial_merge_pushdown_strategy`` is set to ``PUSH_THROUGH_LOW_MEMORY_OPERATORS`` and ``optimize_full_outer_join_with_coalesce`` is set to ``true`` and query has mismatched partition and uses ``FULL OUTER JOIN`` with ``COALESCE``.
* Change the default for configuration property ``use-legacy-scheduler`` to ``true`` in order to mitigate a regression in the new scheduler.
* Optimize connection pooling to avoid running out of sockets in certain cases.
* Add support for stopping new query execution when total number of tasks exceeds a set limit to help with reliability. The limit can be set by configuration property ``experimental.max-total-running-task-count-to-not-execute-new-query``.
* Allow forcing streaming exchange for Mark Distinct using 'query.use-streaming-exchange-for-mark-distinct'.
* Add `cast(tile AS bigint)` and `cast(bigint_value AS bingtile)` to encode/decode Bing tiles to/from bigints.  This is a more efficient storage format that also reduces bucket skew in some cases.
* Add KHyperLogLog Type.
* Add MAKE_KHYPERLOGLOG(K, V) -> KHYPERLOGLOG aggregate function.
* Add KHyperLogLog related scalar functions.
* Improve the scale writer heuristics by considering overall producer buffer utilization. This can be enabled by using the session property `optimized_scale_writer_producer_buffer` and the configuration property `optimized-scale-writer-producer-buffer`.

Verifier Changes
________________
* Add support for verifying ``SELECT`` queries that produce structured types containing ``DATE`` or ``UNKNOWN`` (null).
* Add support for verifying ``SELECT`` queries that produce ``DATE`` or ``UNKNOWN`` (null) columns.
* Add support to auto-resolve control check query failures due to ``EXCEEDED_TIME_LIMIT``.
* Support determinism analysis for simple queries with top-level ``ORDER BY LIMIT`` clause. (:pr:`14181`).

SPI Changes
___________
* All the methods in ``SystemAccessControl`` now take additional parameter ``AccessControlContext context``.

Druid Changes
_____________
* Aggregation Pushdown for Druid connector.
* Add LIMIT evaluation pushdown to Druid connector.
