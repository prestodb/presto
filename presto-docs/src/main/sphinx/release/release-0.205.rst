=============
Release 0.205
=============

General Changes
---------------

* Fix parsing of row types where the field types contain spaces.
  Previously, row expressions that included spaces would fail to parse.
  For example: ``cast(row(timestamp '2018-06-01') AS row(timestamp with time zone))``.
* Fix distributed planning failure for complex queries when using bucketed execution.
* Fix :func:`ST_ExteriorRing` to only accept polygons.
  Previously, it erroneously accepted other geometries.
* Add the ``task.min-drivers-per-task`` and ``task.max-drivers-per-task`` config options.
  The former specifies the guaranteed minimum number of drivers a task will run concurrently
  given that it has enough work to do. The latter specifies the maximum number of drivers
  a task can run concurrently.
* Add the ``concurrent-lifespans-per-task`` config property to control the default value
  of the ``concurrent_lifespans_per_task`` session property.
* Add the ``query_max_total_memory`` session property and the ``query.max-total-memory``
  config property. Queries will be aborted after their total (user + system) memory
  reservation exceeds this threshold.
* Improve stats calculation for outer joins and correlated subqueries.
* Reduce memory usage when a ``Block`` contains all null or all non-null values.
* Change the internal hash function used in  ``approx_distinct``. The result of ``approx_distinct``
  may change in this version compared to the previous version for the same set of values. However,
  the standard error of the results should still be within the configured bounds.
* Improve efficiency and reduce memory usage for scalar correlated subqueries with aggregations.
* Remove the legacy local scheduler and associated configuration properties,
  ``task.legacy-scheduling-behavior`` and ``task.level-absolute-priority``.
* Do not allow using the ``FILTER`` clause for the ``COALESCE``, ``IF``, or ``NULLIF`` functions.
  The syntax was previously allowed but was otherwise ignored.

Security Changes
----------------

* Remove unnecessary check for ``SELECT`` privileges for ``DELETE`` queries.
  Previously, ``DELETE`` queries could fail if the user only has ``DELETE``
  privileges but not ``SELECT`` privileges.
  This only affected connectors that implement ``checkCanSelectFromColumns()``.
* Add a check that the view owner has permission to create the view when
  running ``SELECT`` queries against a view. This only affected connectors that
  implement ``checkCanCreateViewWithSelectFromColumns()``.
* Change ``DELETE FROM <table> WHERE <condition>`` to check that the user has ``SELECT``
  privileges on the objects referenced by the ``WHERE`` condition as is required by the SQL standard.
* Improve the error message when access is denied when selecting from a view due to the
  view owner having insufficient permissions to create the view.

JDBC Driver Changes
-------------------

* Add support for prepared statements.
* Add partial query cancellation via ``partialCancel()`` on ``PrestoStatement``.
* Use ``VARCHAR`` rather than ``LONGNVARCHAR`` for the Presto ``varchar`` type.
* Use ``VARBINARY`` rather than ``LONGVARBINARY`` for the Presto ``varbinary`` type.

Hive Connector Changes
----------------------

* Improve the performance of ``INSERT`` queries when all partition column values are constants.
* Improve stripe size estimation for the optimized ORC writer.
  This reduces the number of cases where tiny ORC stripes will be written.
* Respect the ``skip.footer.line.count`` Hive table property.

CLI Changes
-----------

* Prevent the CLI from crashing when running on certain 256 color terminals.

SPI Changes
-----------

* Add a context parameter to the ``create()`` method in ``SessionPropertyConfigurationManagerFactory``.
* Disallow non-static methods to be annotated with ``@ScalarFunction``. Non-static SQL function
  implementations must now be declared in a class annotated with ``@ScalarFunction``.
* Disallow having multiple public constructors in ``@ScalarFunction`` classes. All non-static
  implementations of a SQL function will now be associated with a single constructor.
  This improves support for providing specialized implementations of SQL functions with generic arguments.
* Deprecate ``checkCanSelectFromTable/checkCanSelectFromView`` and
  ``checkCanCreateViewWithSelectFromTable/checkCanCreateViewWithSelectFromView`` in ``ConnectorAccessControl``
  and ``SystemAccessControl``. ``checkCanSelectFromColumns`` and ``checkCanCreateViewWithSelectFromColumns``
  should be used instead.

.. note::

    These are backwards incompatible changes with the previous SPI.
    If you have written a plugin using these features, you will need
    to update your code before deploying this release.
