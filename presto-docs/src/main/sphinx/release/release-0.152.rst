=============
Release 0.152
=============

General Changes
---------------

* Add :func:`array_union` function.
* Add :func:`reverse` function for arrays.
* Fix issue that could cause queries with ``varchar`` literals to fail.
* Fix query failure when referencing a field of a ``NULL`` row.
* Improve query performance for multiple consecutive window functions.
* Prevent web UI from breaking when query fails without an error code.
* Display port on the task list in the web UI when multiple workers share the same host.
* Add support for ``EXCEPT``.
* Rename ``FLOAT`` type to ``REAL`` for better compatibility with the SQL standard.

Hive Changes
------------

* Add file based security, which can be configured with the ``hive.security``
  and ``security.config-file`` config properties. See :doc:`/connector/hive-security`
  for more details.

Verifier Changes
----------------

* Fix handling of shadow write queries with a ``LIMIT``.

Local File Changes
------------------

* Fix file descriptor leak.
