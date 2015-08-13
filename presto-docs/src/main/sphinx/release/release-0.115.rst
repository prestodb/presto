=============
Release 0.115
=============

General Changes
---------------

* Fix an issue with hierarchical queue rules where queries could be rejected after being accepted.

Hive Changes
------------

* Fix a race condition which could cause queries to finish without reading all the data.
* Fix a bug in Parquet reader that causes failures while reading lists that has an element
  schema name other than ``array_element`` in its Parquet-level schema.
