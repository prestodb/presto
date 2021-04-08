===============
Druid Connector
===============

Overview
--------

The Druid Connector allows access to Druid data from Presto.
This document describes how to setup the Druid Connector to run SQL queries against Druid.

.. note::

    It is highly recommended to use Druid 0.17.0 or later.

Configuration
-------------

To configure the Druid connector, create a catalog properties file
``etc/catalog/druid.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=druid
    druid.coordinator-url=hostname:port
    druid.broker-url=hostname:port
    druid.schema-name=schema
    druid.compute-pushdown-enabled=true

Configuration Properties
------------------------

The following configuration properties are available:

=================================================== ============================================================
Property Name                                        Description
=================================================== ============================================================
``druid.coordinator-url``                            Druid coordinator url.
``druid.broker-url``                                 Druid broker url.
``druid.schema-name``                                Druid schema name.
``druid.compute-pushdown-enabled``                   Whether to pushdown all query processing to Druid.
``druid.case-insensitive-name-matching``             Match dataset and table names case-insensitively
``druid.case-insensitive-name-matching.cache-ttl``   Duration for which remote dataset and table names will be
                                                     cached. Set to ``0ms`` to disable the cache
==================================================== ============================================================

``druid.coordinator-url``
^^^^^^^^^^^^^^^^^^^^^^^^^

Druid coordinator url, e.g. localhost:8081.

``druid.broker-url``
^^^^^^^^^^^^^^^^^^^^

Druid broker url, e.g. localhost:8082.

``druid.schema-name``
^^^^^^^^^^^^^^^^^^^^^

Druid schema name.

This property is optional; the default is ``druid``.

``druid.compute-pushdown-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Whether to pushdown all query processing to Druid.

the default is ``false``.

``druid.case-insensitive-name-matching``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Match dataset and table names case-insensitively.

The default is ``false``.

``druid.case-insensitive-name-matching.cache-ttl``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Duration for which remote dataset and table names will be cached. Set to ``0ms`` to disable the cache.

The default is ``1m``.

Data Types
----------

The data type mappings are as follows:

=============== =============
Druid           Presto
=============== =============
``VARCHAR``     ``VARCHAR``
``BIGINT``      ``BIGINT``
``DOUBLE``      ``DOUBLE``
``FLOAT``       ``REAL``
``TIMESTAMP``   ``TIMESTAMP``
(others)        (unsupported)
=============== =============
