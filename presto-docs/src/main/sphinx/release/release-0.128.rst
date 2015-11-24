=============
Release 0.128
=============

General Changes
---------------

* Fix cast from json to structural types when rows or maps have arrays,
  rows, or maps nested in them.
* Fix Example HTTP connector.
  It would previously fail with a JSON deserialization error.
* Optimize memory usage in TupleDomain.
