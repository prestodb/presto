=============
Release 0.127
=============

General Changes
---------------

* Disable index join repartitioning when it disrupts streaming execution.
* Fix memory accounting leak in some ``JOIN`` queries.
