=============
Release 0.217
=============

General Changes
---------------

* Fix poor query planning performance for queries submitted when the cluster does not have the minimum required number of workers available.
* Fix a bug that caused a union of a table with a ``VALUES`` statement to execute on a single machine, which may result in out of memory errors.
* Improve performance of some queries that use window functions by eliminating redundant shuffles.
* Add grouped execution support for window functions.
* Add support for the ``BOOLEAN`` type for ``EXPLAIN IO`` statements.
* Add support for choosing distribution type for semi joins based on estimated cost when the session property ``join_distribution_type='AUTOMATIC'`` is set.
* Add support for collecting table statistics on demand with the ``ANALYZE`` statement.
* Add a config option (``query.stage-count-warning-threshold``) to specify a per-query threshold for the number of stages.
  When this threshold is exceeded, a ``TOO_MANY_STAGES`` warning is raised.
* Add per-task peak user memory usage to query statistics.
* Add CLI support for showing the amount of data spilled during query execution.
* Add :func:`ST_Points` function.
* Add :doc:`/connector/elasticsearch`.
* Remove the system memory pool and related configuration properties (``resources.reserved-system-memory``, ``deprecated.legacy-system-pool-enabled``) entirely.
  System memory pool was deprecated in 0.201, and it was unused by default since that release. All memory allocations will now be served from the general/user memory pool.


Web UI
------

* Improve live plan to show data transfer statistics as edge labels.
* Improve live plan to contain more information about each node. For example, table scan nodes now show the name of the tables that are being scanned.
* Add the ability to zoom when viewing plans for completed queries.
* Add support for showing the amount of data spilled during query execution.


Hive Connector Changes
----------------------

* Fix an issue where a partially successful rollback of a write could cause data loss and corrupt the metastore.
  The ``hive.skip-target-cleanup-on-rollback`` configuration property can be used to skip deleting target directories when partition creation is rolled back.
* Fix an issue where creating a table on S3 could fail for S3 prefixes without any associated objects (e.g., empty S3 directories).
* Add support for ``ANALYZE`` statement in the Hive connector.
  It's possible to specify a list of partitions to collect statistics for using the ``WITH`` properties of the ``ANALYZE`` statement.
* Add configuration property ``hive.temporary-staging-directory-enabled`` and session property ``temporary_staging_directory_enabled``
  to control whether a temporary staging directory should be used for write operations.
* Add configuration property ``hive.temporary-staging-directory-path`` and session property ``temporary_staging_directory_path``
  to control the location of temporary staging directory that is used for write operations.
  The ``${USER}`` placeholder can be used to use a different location for each user (e.g., ``/tmp/${USER}``).


JDBC Connector Changes
----------------------

* Add support for defining procedures.
* Add support for providing table statistics.


SPI Changes
-----------

* Add new SPIs for the ``ANALYZE`` statement. By default, running the ``ANALYZE`` statement for a connector that does not implement these SPIs results in
  an error o``USER_ERROR`` with error code ``NOT_SUPPORTED``.
