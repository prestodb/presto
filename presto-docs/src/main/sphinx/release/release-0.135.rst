=============
Release 0.135
=============

General Changes
---------------

* Add summary of change in CPU usage to verifier output.
* Add cast between JSON and VARCHAR, BOOLEAN, DOUBLE, BIGINT. For the old
  behavior of cast between JSON and VARCHAR (pre-:doc:`/release/release-0.122`),
  use :func:`json_parse` and :func:`json_format`.
* Fix bug in 0.134 that prevented query page in web UI from displaying in
  Safari.
