=============
Release 0.130
=============

General Changes
---------------

* Add :func:`map_concat` function.
* Performance improvements for filters, projections and dictionary encoded data.
  This optimization is turned off by default. It can be configured via the
  ``optimizer.columnar-processing-dictionary`` config property or the
  ``columnar_processing_dictionary`` session property.
