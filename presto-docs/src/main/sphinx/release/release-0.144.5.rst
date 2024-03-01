===============
Release 0.144.5
===============

General Changes
---------------

* Fix window functions to correctly handle empty frames between unbounded and
  bounded in the same direction. For example, a frame such as
  ``ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING``
  would incorrectly use the first row as the window frame for the first two
  rows rather than using an empty frame.
* Fix correctness issue when grouping on columns that are also arguments to aggregation functions.
