=============
Release 0.140
=============

General Changes
---------------

* Optimize predicate expressions to minimize redundancies.

Hive Changes
------------

* Remove bogus "from deserializer" column comments.

SPI Changes
-----------

* Remove partition key from ``ColumnMetadata``.
* Change return type of ``ConnectorTableLayout.getDiscretePredicates()``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
