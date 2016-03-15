=============
Release 0.142
=============

General Changes
---------------

* Fix planning bug for ``JOIN`` criteria that optimizes to a ``FALSE`` expression.

Hive Changes
------------

* Change ORC input format to report actual bytes read as opposed to estimated bytes.
* Fix cache invalidation when renaming tables.
