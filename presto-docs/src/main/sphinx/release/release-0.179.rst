=============
Release 0.179
=============

General Changes
---------------

* Fix issue which could cause incorrect results when processing dictionary encoded data.
  If the expression can fail on bad input, the results from filtered-out rows containing
  bad input may be included in the query output (:issue:`x8262`).
* Fix planning failure when similar expressions appear in the ``ORDER BY`` clause of a query that
  contains ``ORDER BY`` and ``LIMIT``.
* Fix planning failure when ``GROUPING()`` is used with the ``legacy_order_by`` session property set to ``true``.
* Fix parsing failure when ``NFD``, ``NFC``, ``NFKD`` or ``NFKC`` are used as identifiers.
* Fix a memory leak on the coordinator that manifests itself with canceled queries.
* Fix excessive GC overhead caused by captured lambda expressions.
* Reduce the memory usage of map/array aggregation functions.
* Redact sensitive config property values in the server log.
* Update timezone database to version 2017b.
* Add :func:`repeat` function.
* Add :func:`crc32` function.
* Add file based global security, which can be configured with the ``etc/access-control.properties``
  and ``security.config-file`` config properties. See :doc:`/security/built-in-system-access-control`
  for more details.
* Add support for configuring query runtime and queueing time limits to resource groups.

Hive Changes
------------

* Fail queries that access encrypted S3 objects that do not have their unencrypted content lengths set in their metadata.

JDBC Driver Changes
-------------------

* Add support for setting query timeout through ``Statement.setQueryTimeout()``.

SPI Changes
-----------

* Add grantee and revokee to ``GRANT`` and ``REVOKE`` security checks.
