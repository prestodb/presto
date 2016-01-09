=============
Release 0.133
=============

General Changes
---------------

* Add support for calling connector-defined procedures using :doc:`/sql/call`.
* Add :doc:`/connector/system` procedure for killing running queries.
* Properly expire idle transactions that consist of just the start transaction statement
  and nothing else.
