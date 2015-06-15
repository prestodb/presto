=============
Release 0.107
=============

General Changes
---------------

* Added ``query_max_memory`` session property. Note: this session property cannot
  increase the limit above the limit set by the ``query.max-memory`` configuration option.
* Fixed task leak caused by queries that finish early, such as a ``LIMIT`` query
  or cancelled query, when the cluster is under high load.
* Added ``task.info-refresh-max-wait`` to configure task info freshness.
* Add support for ``DELETE`` to language and connector SPI.
* Reenable error classification code for syntax errors.
* Fix out of bounds exception in :func:`lower` and :func:`upper`
  when the string contains the code point ``U+10FFFF``.
