============
Release 0.99
============

General Changes
---------------
* Reduce lock contention in ``TaskExecutor``.
* Fix reading maps with null keys from ORC.
* Fix precomputed hash optimization for nulls values.
* Make :func:`contains()` work for all comparable types.
