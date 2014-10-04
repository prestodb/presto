============
Release 0.78
============

ARRAY and MAP Types in Hive Connector
-------------------------------------

The Hive connector now returns arrays and maps instead of json encoded strings,
for columns whose underlying type is array or map. Please note that this is a backwards
incompatible change, and the :ref:`json_functions` will no longer work on these columns,
unless you :func:`cast` them to the ``json`` type.

General Changes
---------------

* Fix expression optimizer, so that it runs in linear time instead of exponential time.
* Add :func:`cardinality` for maps
