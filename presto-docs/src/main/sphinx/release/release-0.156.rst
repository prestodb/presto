=============
Release 0.155
=============

SPI Changes
-----------

* Add ``checkCanAccessCatalog()`` method to both ``ConnectorAccessControl`` and ``SystemAccessControl``.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector and implemented ConnectorAccessControl or SystemAccessControl, you will need to update your code
    before deploying this release.
