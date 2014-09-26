============
Release 0.77
============

Parametric Types
----------------
Presto now has a framework for implementing parametric types and functions.
Support for :ref:`array_type` and :ref:`map_type` types has been added, including the element accessor
operator ``[]``, and new :ref:`array_functions`.

Hive Changes
------------

* Handle spurious ``AbortedException`` when closing S3 input streams
