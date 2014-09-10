============
Release 0.80
============

Hive Changes
------------

* The maximum retry time for the Hive S3 file system can be configured
  by setting ``hive.s3.max-retry-time``.

General Changes
---------------

* Add support implicit joins. The following syntax is now allowed::

    SELECT * FROM t, u

* Add property ``task.verbose-stats`` to enable verbose statistics collection for
  tasks. The default is ``false``.

* Format binary data in the CLI as a hex dump.

