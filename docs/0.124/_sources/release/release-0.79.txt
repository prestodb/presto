============
Release 0.79
============

Hive Changes
------------

* Add configuration option ``hive.force-local-scheduling`` and session property
  ``force_local_scheduling`` to force local scheduling of splits.
* Add new experimental optimized RCFile reader.  The reader can be enabled by
  setting the configuration option ``hive.optimized-reader.enabled`` or session
  property ``optimized_reader_enabled``.

General Changes
---------------

* Add support for :ref:`unnest`, which can be used as a replacement for the `explode()` function in Hive.
* Fix a bug in the scan operator that can cause data to be missed. It currently only affects queries
  over ``information_schema`` or ``sys`` tables, metadata queries such as ``SHOW PARTITIONS`` and connectors
  that implement the ``ConnectorPageSource`` interface.
