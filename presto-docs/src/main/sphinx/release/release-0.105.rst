=============
Release 0.105
=============

General Changes
---------------

* Fix issue which can cause queries to be blocked permanently.
* Close connections correctly in JDBC connectors.
* Add implicit coercions for values of equi-join criteria.
* Fix detection of window function calls without an ``OVER`` clause.

SPI Changes
-----------

* Remove ``ordinalPosition`` from ``ColumnMetadata``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
