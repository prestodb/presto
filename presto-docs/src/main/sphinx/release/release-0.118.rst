=============
Release 0.118
=============

General Changes
---------------

* Fix planning error for ``UNION`` queries that require implicit coercions.
* Fix null pointer exception when using :func:`checksum`.
* Fix completion condition for ``SqlTask`` that can cause queries to be blocked.

Authorization
-------------

We've added experimental support for authorization of SQL queries in Presto.
This is currently only supported by the Hive connector. You can enable Hive
checks by setting the ``hive.security`` property to ``none``, ``read-only``,
or ``sql-standard``.

.. note::

    The authentication support is experimental and only lightly tested. We are
    actively working on this feature, so expect backwards incompatible changes.
    See the ``ConnectorAccessControl`` interface the SPI for details.

