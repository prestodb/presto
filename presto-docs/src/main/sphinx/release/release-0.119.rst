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
* Execute views with the permissions of the view owner.
* Add owner to view metadata.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector that supports views, you will need to
    update your code to the new APIs.


CLI Changes
-----------

* Fix handling of full width characters.
* Skip printing query URL if terminal is too narrow.
* Allow performing a partial query cancel using ``ctrl-P``.
* Allow toggling debug mode during query by pressing ``D``.

Hive Changes
------------

* Add ``ALTER TABLE ADD COLUMN``
* Add ``ALTER TABLE RENAME COLUMN``
* Automatically configure the S3 region when running in EC2.
* Allow configuring multiple Hive metastores for high availability.

MySQL and PostgreSQL Changes
----------------------------

* Enable streaming results instead of buffering everything in memory.
* Fix handling of pattern characters when matching table or column names.
