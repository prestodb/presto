=============
Release 0.185
=============

General Changes
---------------

* Fix incorrect column names in ``QueryCompletedEvent``.
* Fix excessive CPU usage in coordinator for queries that have
  large string literals containing non-ASCII characters.
* Do not allow dropping or renaming hidden columns.
* When preparing to drop a column, ignore hidden columns when
  checking if the table only has one column.
* Improve performance of joins where the condition is a range over a function.
  For example: ``a JOIN b ON b.x < f(a.x) AND b.x > g(a.x)``
* Extend :func:`substr` function to work on ``VARBINARY`` in addition to ``CHAR`` and ``VARCHAR``.
* Add cast from ``JSON`` to ``ROW``.
* The ``TRY`` function is now allowed in lambda expressions.

Hive Changes
------------

* Improve ORC reader efficiency by only reading small ORC streams when accessed in the query.
* Improve RCFile IO efficiency by increasing the buffer size from 1 to 8 MB.
* Fix native memory leak for optimized RCFile writer.
* Fix potential native memory leak for optimized ORC writer.

Memory Connector Changes
------------------------

* Add support for views.
