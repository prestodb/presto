=============
Release 0.108
=============

General Changes
---------------

* Fix incorrect query results when one window function follows another row_number() window function and are
  both partitioned on the same columns.
