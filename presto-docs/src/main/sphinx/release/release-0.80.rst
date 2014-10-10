============
Release 0.80
============

General Changes
---------------

* Add support implicit joins. The following syntax is now allowed::

    SELECT * FROM t, u

* Add property ``task.verbose-stats`` to enable verbose statistics collection for
  tasks. The default is ``false``.

* Format binary data in the CLI as a hex dump.

