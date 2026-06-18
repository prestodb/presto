============
Release 0.59
============

* Fix hang in ``HiveSplitSource``.  A query over a large table can hang
  in split discovery due to a bug introduced in 0.57.
