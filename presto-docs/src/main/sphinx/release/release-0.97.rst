============
Release 0.97
============

General Changes
---------------

* The queueing policy in Presto can now be injected. See :doc:`/admin/queue` for details.
* Speed up detection of ASCII strings in implementation of ``LIKE`` operator.
* Fix NullPointerException when metadata-based query optimization is enabled.
* Fix possible infinite loop when decompressing ORC data.
* Fix an issue where ``NOT`` clause was being ignored in ``NOT BETWEEN`` predicates.
* Fix a planning issue in queries that use ``SELECT *``, window functions and implicit coercions.
