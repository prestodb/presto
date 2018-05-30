===============
Release 0.144.2
===============

General Changes
---------------

* Fix potential memory leak in coordinator query history.
* Add ``driver.max-page-partitioning-buffer-size`` config to control buffer size
  used to repartition pages for exchanges.
