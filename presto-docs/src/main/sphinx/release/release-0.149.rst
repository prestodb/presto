=============
Release 0.149
=============

General Changes
---------------
* ``query.max-age`` property was renamed to ``query.min-expire-age``.
* ``optimizer.columnar-processing`` and ``optimizer.columnar-processing-dictionary``
  properties were merged to ``optimizer.processing-optimization`` with possible
  values ``disabled``, ``columnar`` and ``columnar_dictionary``
* ``columnar_processing`` and ``columnar_processing_dictionary`` session
  properties were merged to ``processing_optimization`` with possible values
  ``disabled``, ``columnar`` and ``columnar_dictionary``
