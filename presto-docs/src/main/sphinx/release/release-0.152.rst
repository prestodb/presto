=============
Release 0.152
=============

General Changes
---------------

* Add :func:`array_union` function.
* Fix issue that could cause queries with ``varchar`` literals to fail.

Hive Connector Changes
----------------------

* Add file based security, which can be configured with the ``hive.security``
  and ``security.config-file`` config properties. See :doc:`/connector/hive-security`
  for more details.
