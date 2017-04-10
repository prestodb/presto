=============
Release 0.114
=============

General Changes
---------------

* Fix ``%k`` specifier for :func:`date_format` and :func:`date_parse`.
  It previously used ``24`` rather than ``0`` for the midnight hour.

Hive Changes
------------

* Fix ORC reader for Hive connector.
