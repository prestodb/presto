=============
Release 0.133
=============

General Changes
---------------

* Add support for calling connector-defined procedures using :doc:`/sql/call`.
* Add :doc:`/connector/system` procedure for killing running queries.
* Properly expire idle transactions that consist of just the start transaction statement
  and nothing else.
* Fix possible deadlock in worker communication when task restart is detected.
* Performance improvements for aggregations on dictionary encoded data.
  This optimization is turned off by default. It can be configured via the
  ``optimizer.dictionary-aggregation`` config property or the
  ``dictionary_aggregation`` session property.
* Fix race which could cause queries to fail when using :func:`concat` on
  :ref:`array_type`, or when enabling ``columnar_processing_dictionary``.
* Add sticky headers and the ability to sort the tasks table on the query page
  in the web interface.
