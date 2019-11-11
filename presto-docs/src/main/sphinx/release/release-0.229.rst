=============
Release 0.229
=============

General Changes
_______________
* Add geospatial function line_interpolate_point.
* Improve hash value computation for structrual types to avoid collision.
* Add support for ``CREATE FUNCTION``.
* Added configs to use common HTTPS settings for internal communications.
* Add configuration property ``experimental.internal-communication.max-task-update-size`` to limit the size of the taskUpdate.
* Add peak total memory distribution among tasks of each stage.
* Respect stage.max-tasks-per-stage to limit number of tasks for scan.
* Move `SetOperationNode` to SPI. Connectors can now alter query plan to have union, intersect, and except.
* Start forwarding X_Forwarded_For in header from Proxy.

SPI Changes
___________
* Added a new Pinot connector.
* Split `ConnectorPlanOptimizerProvider` to have two phases for connectors to participate query optimization: `LOGICAL` and `PHYSICAL`. The two phases correspond to pre- and post-shuffle optimization respectively.

Hive Changes
____________
* Fix parquet predicate pushdown on dictionaries to consider more than just the first predicate column.
* Improve parquet predicate pushdown on dictionaries to avoid reading additional data after successfully eliminating a block.

Raptor Changes
______________
* Change `storage.data-directory` from path to URI. For existing deployment on local flash, a scheme header "file://" should be added to the original config value.
* Change error code name `RAPTOR_LOCAL_FILE_SYSTEM_ERROR` to `RAPTOR_FILE_SYSTEM_ERROR`.
