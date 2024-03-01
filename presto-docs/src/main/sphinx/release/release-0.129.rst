=============
Release 0.129
=============

.. warning::

   There is a performance regression in this release for ``GROUP BY`` and ``JOIN``
   queries when the length of the keys is between 16 and 31 bytes. This is fixed
   in :doc:`/release/release-0.130`.

General Changes
---------------

* Fix a planner issue that could cause queries involving ``OUTER JOIN`` to
  return incorrect results.
* Some queries, particularly those using :func:`max_by` or :func:`min_by`, now
  accurately reflect their true memory usage and thus appear to use more memory
  than before.
* Fix :doc:`/sql/show-session` to not show hidden session properties.
* Fix hang in large queries with ``ORDER BY`` and ``LIMIT``.
* Fix an issue when casting empty arrays or arrays containing only ``NULL`` to
  other types.
* Table property names are now properly treated as case-insensitive.
* Minor UI improvements for query detail page.
* Do not display useless stack traces for expected exceptions in verifier.
* Improve performance of queries involving ``UNION ALL`` that write data.
* Introduce the ``P4HyperLogLog`` type, which uses an implementation of the HyperLogLog data
  structure that trades off accuracy and memory requirements when handling small sets for an
  improvement in performance.

JDBC Driver Changes
-------------------

* Throw exception when using :doc:`/sql/set-session` or :doc:`/sql/reset-session`
  rather than silently ignoring the command.
* The driver now properly supports non-query statements.
  The ``Statement`` interface supports all variants of the ``execute`` methods.
  It also supports the ``getUpdateCount`` and ``getLargeUpdateCount`` methods.

CLI Changes
-----------

* Always clear screen when canceling query with ``ctrl-C``.
* Make client request timeout configurable.

Network Topology Aware Scheduling
---------------------------------

The scheduler can now be configured to take network topology into account when
scheduling splits. This is set using the ``node-scheduler.network-topology``
config. See :doc:`/admin/tuning` for more information.

Hive Changes
------------

* The S3 region is no longer automatically configured when running in EC2.
  To enable this feature, use ``hive.s3.pin-client-to-current-region=true``
  in your Hive catalog properties file. Enabling this feature is required
  to access S3 data in the China isolated region, but prevents accessing
  data outside the current region.
* Server-side encryption is now supported for S3. To enable this feature,
  use ``hive.s3.sse.enabled=true`` in your Hive catalog properties file.
* Add support for the ``retention_days`` table property.
* Add support for S3 ``EncryptionMaterialsProvider``.
