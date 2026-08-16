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

======================================== =========================================================
Property Name                            Description
======================================== =========================================================
``druid.coordinator-url``                Druid coordinator URL.
``druid.broker-url``                     Druid broker URL.
``druid.schema-name``                    Druid schema name.
``druid.compute-pushdown-enabled``       Whether to pushdown all query processing to Druid.
``case-sensitive-name-matching``         Enable case-sensitive identifier support for schema,
                                         table, and column names for the connector. When disabled,
                                         names are matched case-insensitively using lowercase
                                         normalization. Default is ``false``.
``druid.tls.enabled``                    Enable TLS when connecting to Druid.
``druid.tls.truststore-path``            Path to the trust certificate file.
``druid.tls.truststore-password``        Password for the trust certificate file.
``druid.authentication.type``            Authentication type for Druid.
``druid.basic.authentication.username``  Username for basic authentication.
``druid.basic.authentication.password``  Password for basic authentication.
``druid.hadoop.config.resources``        Hadoop configuration resources.
``druid.ingestion.storage.path``         Local storage path for ingestion.
======================================== =========================================================

``druid.coordinator-url``
^^^^^^^^^^^^^^^^^^^^^^^^^

Druid coordinator URL. For example, ``localhost:8081``.

``druid.broker-url``
^^^^^^^^^^^^^^^^^^^^

Druid broker URL. For example, ``localhost:8082``.

``druid.schema-name``
^^^^^^^^^^^^^^^^^^^^^

Druid schema name.

This property is optional; the default is ``druid``.

``druid.compute-pushdown-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Whether to pushdown all query processing to Druid.

The default is ``false``.

``druid.tls.enabled``
^^^^^^^^^^^^^^^^^^^^^

Enable TLS when connecting to Druid.

The default is ``false``.

``druid.tls.truststore-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Path to the trust certificate file.

``druid.tls.truststore-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Password for the trust certificate file.

``druid.authentication.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Authentication type for Druid.

Supported values are: ``NONE`` (default), ``BASIC`` and ``KERBEROS``.

``druid.basic.authentication.username``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Username for basic authentication. Required when ``druid.authentication.type`` is set to ``BASIC``.

``druid.basic.authentication.password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Password for basic authentication. Required when ``druid.authentication.type`` is set to ``BASIC``.

``druid.hadoop.config.resources``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of Hadoop configuration resource paths (for example, ``core-site.xml``, ``hdfs-site.xml``).
Required if Druid uses HDFS for deep storage or requires Kerberos configuration.

``druid.ingestion.storage.path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Local storage path on Presto nodes used for temporary files during data ingestion.

The default is the system temporary directory (``java.io.tmpdir``).

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
