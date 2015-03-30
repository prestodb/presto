============
Release 0.88
============

General Changes
---------------

* Added :func:`arbitrary` aggregation function.
* Allow using all :doc:`/functions/aggregate` as :doc:`/functions/window`.
* Support specifying window frames and correctly implement frames for all :doc:`/functions/window`.
* Allow :func:`approx_distinct` aggregation function to accept a standard error parameter.
* Implement :func:`least` and :func:`greatest` with variable number of arguments.
* :ref:`array_type` is now comparable and can be used as ``GROUP BY`` keys or in ``ORDER BY`` expressions.
* Implement ``=`` and ``<>`` operators for :ref:`row_type`.
* Fix excessive garbage creation in the ORC reader.
* Fix an issue that could cause queries using :func:`row_number()` and ``LIMIT`` to never terminate.
* Fix an issue that could cause queries with :func:`row_number()` and specific filters to produce incorrect results.
* Fixed an issue that caused the Cassandra plugin to fail to load with a SecurityException.
