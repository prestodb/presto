=============
Release 0.129
=============

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
