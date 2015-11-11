=============
Release 0.126
=============

General Changes
---------------

* Improve handling of physical properties which can increase performance for
  queries involving window functions.
* Fix reset of session properties in CLI when running :doc:`/sql/use`.
* Fix query planning failure that occurs in some cases with the projection
  push down optimizer.
