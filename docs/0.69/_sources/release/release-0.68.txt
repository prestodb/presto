============
Release 0.68
============

* Fix a regression in the handling of Hive tables that are bucketed on a
  string column. This caused queries that could take advantage of bucketing
  on such tables to choose the wrong bucket and thus would not match any
  rows for the table. This regression was introduced in 0.66.

* Fix double counting of bytes and rows when reading records
