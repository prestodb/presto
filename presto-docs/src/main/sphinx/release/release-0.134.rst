=============
Release 0.134
=============

General Changes
---------------

* Add cumulative memory statistics tracking and expose the stat in the web interface.
* Remove nullability and partition key flags from :doc:`/sql/show-columns`.
* Remove non-standard ``is_partition_key`` column from ``information_schema.columns``.
* Fix performance regression in creation of ``DictionaryBlock``.
* Fix rare memory accounting leak in queries with ``JOIN``.

Hive Changes
------------

* The comment for partition keys is now prefixed with *"Partition Key"*.

SPI Changes
-----------

* Remove legacy partition API methods and classes.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector and have not yet updated to the
    ``TableLayout`` API, you will need to update your code before deploying
    this release.
