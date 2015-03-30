============
Release 0.98
============

Array, Map, and Row Types
-------------------------

The memory representation of these types is now ``VariableWidthBlockEncoding``
instead of ``JSON``.

.. note::
    This is a backwards incompatible change with the previous representation,
    so if you have written a connector or function, you will need to update
    your code before deploying this release.

Hive Changes
------------

* Fix handling of ORC files with corrupt checkpoints.

SPI Changes
-----------

* Rename ``Index`` to ``ConnectorIndex``.

.. note::
    This is a backwards incompatible change, so if you have written a connector
    that uses ``Index``, you will need to update your code before deploying this release.

General Changes
---------------

* Fix bug in ``UNNEST`` when output is unreferenced or partially referenced.
* Make :func:`max` and :func:`min` functions work on all orderable types.
* Optimize memory allocation in :func:`max_by` and other places that ``Block`` is used.
