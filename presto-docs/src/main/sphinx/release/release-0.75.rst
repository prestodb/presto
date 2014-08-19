============
Release 0.75
============

Hive Changes
------------

* The Hive S3 file system has a new configuration option,
  ``hive.s3.max-connections``, which sets the maximum number of
  connections to S3.  The default is ``500``.

SPI changes
-----------

The core Presto engine no longer automatically adds a column for ``count(*)``
queries. Instead the ``RecordCursorProvider`` will receive an empty list of
column handles.

.. note::
    If you have written a connector that expects the column handles list to
    not be empty, you will need to update your code before deploying this
    release.

The ``Type`` and ``Block`` APIs have gone through a major refactoring in this
release. The main focus of the refactoring was to consolidate all type specific
encoding logic in the type itself, which makes types much easier to implement.
You should consider ``Type`` and ``Block`` to be a beta API as we expect
further changes in the near future.

.. note::
    This is a backwards incompatible change to ``Type`` and ``Block`` in the
    SPI, so if you have written a ``Type`` or ``Block``, you will need to
    update your code before deploying this release.

General Changes
---------------

* Optimize ``count(constant)`` as ``count(*)`` which is much faster
* Add support for square bracket syntax in :func:`json_extract` functions
* Add support for binary types to the JDBC driver
* The legacy byte code compiler has been removed
* New aggregation framework (~10% faster)
* Added :func:`max_by` aggregation function
* Fixed parsing of UNION queries that use both DISTINCT and ALL
