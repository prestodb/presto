============
Release 0.67
============

* Fix resource leak in Hive connector

* Improve error categorization in event logging

* Fix planning issue with certain queries using window functions

SPI changes
-----------

The ``ConnectorSplitSource`` interface now extends ``Closeable``.

.. note::
    This is a backwards incompatible change to ``ConnectorSplitSource`` in the SPI,
    so if you have written a connector, you will need to update your code before
    deploying this release.
