=============
Release 0.229
=============

General Changes
_______________
* Fix an issue that would cause query failure when calling :func:`geometry_to_bing_tiles` on certain degenerate geometries.
* Add geospatial function :func:`line_interpolate_point`.
* Add support for ``CREATE FUNCTION``
* Add support for passing ``X_Forwarded_For`` header from Proxy to coordinator.
* Add support to respect configuration property ``stage.max-tasks-per-stage`` for limiting the number of tasks per scan.
* Add configuration property ``experimental.internal-communication.max-task-update-size`` to limit the size of the ``TaskUpdate``.
* Add configuration properties ``internal-communication.https.trust-store-path``, ``internal-communication.https.included-cipher``,
  and ``internal-communication.https.excluded-cipher`` to easily set common https configurations for all internal communications at one place.
* Add peak task memory distribution of each stage to ``QueryStatistics``.

Pinot Connector Changes
_______________________
* Add Pinot connector.

Hive Connector Changes
______________________
* Fix parquet predicate pushdown on dictionaries to consider more than just the first predicate column.
* Improve parquet predicate pushdown on dictionaries to avoid reading additional data after successfully eliminating a block.

Raptor Connector Changes
________________________
* Add support for using remote HDFS as the storage in Raptor. Configuration property ``storage.data-directory`` is changed from a ``File`` to a ``URI``.
  For deployment on local flash, scheme ``file:/`` must be prepended.
* Rename error code ``RAPTOR_LOCAL_FILE_SYSTEM_ERROR`` to ``RAPTOR_FILE_SYSTEM_ERROR``.

SPI Changes
___________
* Add support for connectors to alter query plans involving ``UNION``, ``INTERSECT``, and ``EXCEPT``, by moving ``SetOperationNode`` to SPI.
* Improve interface ``ConnectorPlanOptimizerProvider`` to allow connectors to participate in query optimization in two phases, ``LOGICAL`` and ``PHYSICAL``.
  The two phases correspond to post-shuffle and post-shuffle optimization, respectively.
