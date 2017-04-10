=============
Release 0.130
=============

General Changes
---------------

* Fix a performance regression in ``GROUP BY`` and ``JOIN`` queries when the
  length of the keys is between 16 and 31 bytes.
* Add :func:`map_concat` function.
* Performance improvements for filters, projections and dictionary encoded data.
  This optimization is turned off by default. It can be configured via the
  ``optimizer.columnar-processing-dictionary`` config property or the
  ``columnar_processing_dictionary`` session property.
* Improve performance of aggregation queries with large numbers of groups.
* Improve performance for queries that use :ref:`array_type` type.
* Fix querying remote views in MySQL and PostgreSQL connectors.
