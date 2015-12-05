=============
Release 0.129
=============

Network Topology Aware Scheduling
---------------------------------

The scheduler can now be configured to take network topology into account when
scheduling splits. This is set using the ``node-scheduler.network-topology``
config. See :doc:`/admin/tuning` for more information.

General Changes
---------------

* The JDBC driver now properly supports non-query statements.
  The ``Statement`` interface supports all variants of the ``execute`` methods.
  It also supports the ``getUpdateCount`` and ``getLargeUpdateCount`` methods.
* Fix :doc:`/sql/show-session` to not show hidden session properties.

CLI Changes
-----------

* Always clear screen when canceling query with ``ctrl-C``.

Hive Changes
------------

* The S3 region is no longer automatically configured when running in EC2.
  To enable this feature, use ``hive.s3.pin-client-to-current-region=true``
  in your Hive catalog properties file. Enabling this feature is required
  to access S3 data in the China isolated region, but prevents accessing
  data outside the current region.
* Server-side encryption is now supported for S3. To enable this feature,
  use ``hive.s3.sse.enabled=true`` in your Hive catalog properties file.
