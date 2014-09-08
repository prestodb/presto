============
Release 0.75
============

Hive Changes
------------

* The Hive S3 file system has a new configuration option,
  ``hive.s3.max-connections``, which sets the maximum number of
  connections to S3. The default has been increased from ``50`` to ``500``.

* The Hive connector now supports renaming tables. By default, this feature
  is not enabled. To enable it, set ``hive.allow-rename-table=true`` in
  your Hive catalog properties file.

General Changes
---------------

* Optimize :func:`count` with a constant to execute as the much faster ``count(*)``
* Add support for binary types to the JDBC driver
* The legacy byte code compiler has been removed
* New aggregation framework (~10% faster)
* Added :func:`max_by` aggregation function
* The ``approx_avg()`` function has been removed. Use :func:`avg` instead.
* Fixed parsing of ``UNION`` queries that use both ``DISTINCT`` and ``ALL``
* Fixed cross join planning error for certain query shapes
* Added hex and base64 conversion functions for varbinary
* Fix the ``LIKE`` operator to correctly match against values that contain
  multiple lines. Previously, it would stop matching at the first newline.
* Add support for renaming tables using the :doc:`/sql/alter-table` statement.
* Add basic support for inserting data using the :doc:`/sql/insert` statement.
  This is currently only supported for the Raptor connector.

JSON Function Changes
---------------------

The :func:`json_extract` and :func:`json_extract_scalar` functions now support
the square bracket syntax::

    SELECT json_extract(json, '$.store[book]');
    SELECT json_extract(json, '$.store["book name"]');

As part of this change, the set of characters allowed in a non-bracketed
path segment has been restricted to alphanumeric, underscores and colons.
Additionally, colons cannot be used in a un-quoted bracketed path segment.
Use the new bracket syntax with quotes to match elements that contain
special characters.

Scheduler Changes
-----------------

The scheduler now assigns splits to a node based on the current load on the node across all queries.
Previously, the scheduler load balanced splits across nodes on a per query level. Every node can have
``node-scheduler.max-splits-per-node`` splits scheduled on it. To avoid starvation of small queries,
when the node already has the maximum allowable splits, every task can schedule at most
``node-scheduler.max-pending-splits-per-node-per-task`` splits on the node.

Row Number Optimizations
------------------------

Queries that use the :func:`row_number` function are substantially faster
and can run on larger result sets for two types of queries.

Performing a partitioned limit that choses ``N`` arbitrary rows per
partition is a streaming operation. The following query selects
five arbitrary rows from ``orders`` for each ``orderstatus``::

    SELECT * FROM (
        SELECT row_number() OVER (PARTITION BY orderstatus) AS rn,
            custkey, orderdate, orderstatus
        FROM orders
    ) WHERE rn <= 5;

Performing a partitioned top-N that chooses the maximum or minimum
``N`` rows from each partition now uses significantly less memory.
The following query selects the five oldest rows based on ``orderdate``
from ``orders`` for each ``orderstatus``::

    SELECT * FROM (
        SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderdate) AS rn,
            custkey, orderdate, orderstatus
        FROM orders
    ) WHERE rn <= 5;

Use the :doc:`/sql/explain` statement to see if any of these optimizations
have been applied to your query.

SPI Changes
-----------

The core Presto engine no longer automatically adds a column for ``count(*)``
queries. Instead, the ``RecordCursorProvider`` will receive an empty list of
column handles.

The ``Type`` and ``Block`` APIs have gone through a major refactoring in this
release. The main focus of the refactoring was to consolidate all type specific
encoding logic in the type itself, which makes types much easier to implement.
You should consider ``Type`` and ``Block`` to be a beta API as we expect
further changes in the near future.

To simplify the API, ``ConnectorOutputHandleResolver`` has been merged into
``ConnectorHandleResolver``. Additionally, ``ConnectorHandleResolver``,
``ConnectorRecordSinkProvider`` and ``ConnectorMetadata`` were modified to
support inserts.

.. note::
    This is a backwards incompatible change with the previous connector and
    type SPI, so if you have written a connector or type, you will need to update
    your code before deploying this release. In particular, make sure your
    connector can handle an empty column handles list (this can be verified
    by running ``SELECT count(*)`` on a table from your connector).
