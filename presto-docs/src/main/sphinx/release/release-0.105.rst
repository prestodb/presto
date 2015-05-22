=============
Release 0.105
=============

SPI Changes
-----------

* Remove ``ordinalPosition`` from ``ColumnMetadata``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector, you will need to update your code
    before deploying this release.
