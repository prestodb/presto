=============
Release 0.117
=============

General Changes
---------------

* Add back casts between JSON and VARCHAR to provide an easier migration path
  to :func:`json_parse` and :func:`json_format`. These will be removed in a
  future release.
