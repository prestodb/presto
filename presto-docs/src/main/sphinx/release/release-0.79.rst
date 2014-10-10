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
