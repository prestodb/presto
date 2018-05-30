=============
Release 0.119
=============

General Changes
---------------

* Add :doc:`/connector/redis`.
* Add :func:`geometric_mean` function.
* Fix restoring interrupt status in ``StatementClient``.
* Support getting server version in JDBC driver.
* Improve correctness and compliance of JDBC ``DatabaseMetaData``.
* Catalog and schema are now optional on the server. This allows connecting
  and executing metadata commands or queries that use fully qualified names.
  Previously, the CLI and JDBC driver would use a catalog and schema named
  ``default`` if they were not specified.
* Fix scheduler handling of partially canceled queries.
* Execute views with the permissions of the view owner.
* Replaced the ``task.http-notification-threads`` config option with two
  independent options: ``task.http-response-threads`` and ``task.http-timeout-threads``.
* Improve handling of negated expressions in join criteria.
* Fix :func:`arbitrary`, :func:`max_by` and :func:`min_by` functions when used
  with an array, map or row type.
* Fix union coercion when the same constant or column appears more than once on
  the same side.
* Support ``RENAME COLUMN`` in :doc:`/sql/alter-table`.

SPI Changes
-----------

* Add more system table distribution modes.
* Add owner to view metadata.

.. note::
    These are backwards incompatible changes with the previous connector SPI.
    If you have written a connector, you may need to update your code to the
    new APIs.


CLI Changes
-----------

* Fix handling of full width characters.
* Skip printing query URL if terminal is too narrow.
* Allow performing a partial query cancel using ``ctrl-P``.
* Allow toggling debug mode during query by pressing ``D``.
* Fix handling of query abortion after result has been partially received.
* Fix handling of ``ctrl-C`` when displaying results without a pager.

Verifier Changes
----------------

* Add ``expected-double-precision`` config to specify the expected level of
  precision when comparing double values.
* Return non-zero exit code when there are failures.

Cassandra Changes
-----------------

* Add support for Cassandra blob types.

Hive Changes
------------

* Support adding and renaming columns using :doc:`/sql/alter-table`.
* Automatically configure the S3 region when running in EC2.
* Allow configuring multiple Hive metastores for high availability.
* Add support for ``TIMESTAMP`` and ``VARBINARY`` in Parquet.

MySQL and PostgreSQL Changes
----------------------------

* Enable streaming results instead of buffering everything in memory.
* Fix handling of pattern characters when matching table or column names.
