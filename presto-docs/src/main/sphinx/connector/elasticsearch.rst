=======================
Elasticsearch Connector
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The Elasticsearch Connector allows access to Elasticsearch data from Presto.
This document describes how to setup the Elasticsearch Connector to run SQL queries against Elasticsearch.

.. note::

    It is highly recommended to use Elasticsearch 6.0.0 or later.

Configuration
-------------

To configure the Elasticsearch connector, create a catalog properties file
``etc/catalog/elasticsearch.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=elasticsearch
    elasticsearch.default-schema=default
    elasticsearch.table-description-directory=etc/elasticsearch/
    elasticsearch.scroll-size=1000
    elasticsearch.scroll-timeout=2s
    elasticsearch.request-timeout=2s
    elasticsearch.max-request-retries=5
    elasticsearch.max-request-retry-time=10s

Configuration Properties
------------------------

The following configuration properties are available:

============================================= ==============================================================================
Property Name                                 Description
============================================= ==============================================================================
``elasticsearch.default-schema``              Default schema name for tables.
``elasticsearch.table-description-directory`` Directory containing JSON table description files.
``elasticsearch.scroll-size``                 Maximum number of hits to be returned with each Elasticsearch scroll request.
``elasticsearch.scroll-timeout``              Amount of time Elasticsearch will keep the search context alive for scroll requests.
``elasticsearch.max-hits``                    Maximum number of hits a single Elasticsearch request can fetch.
``elasticsearch.request-timeout``             Timeout for Elasticsearch requests.
``elasticsearch.max-request-retries``         Maximum number of Elasticsearch request retries.
``elasticsearch.max-request-retry-time``      Use exponential backoff starting at 1s up to the value specified by this configuration when retrying failed requests.
============================================= ==============================================================================

``elasticsearch.default-schema``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema that will contain all tables defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``elasticsearch.table-description-directory``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies a path under the Presto deployment directory that contains
one or more JSON files with table descriptions (must end with ``.json``).

This property is optional; the default is ``etc/elasticsearch``.

``elasticsearch.scroll-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum number of hits that can be returned with each
Elasticsearch scroll request.

This property is optional; the default is ``1000``.

``elasticsearch.scroll-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the amount of time Elasticsearch will keep the `search context alive`_ for scroll requests

This property is optional; the default is ``1s``.

.. _search context alive: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#scroll-search-context

``elasticsearch.max-hits``
^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum number of `hits`_ an Elasticsearch request can fetch.

This property is optional; the default is ``1000000``.

.. _hits: https://www.elastic.co/guide/en/elasticsearch/reference/current/search.html

``elasticsearch.request-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the timeout value for all Elasticsearch requests.

This property is optional; the default is ``10s``.

``elasticsearch.max-request-retries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum number of Elasticsearch request retries.

This property is optional; the default is ``5``.

``elasticsearch.max-request-retry-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use exponential backoff starting at 1s up to the value specified by this configuration when retrying failed requests.

This property is optional; the default is ``10s``.

Search Guard Authentication
---------------------------

The Elasticsearch connector provides additional security options to support Elasticsearch clusters that have been configured to use Search Guard.

You can configure the certificate format by setting the ``searchguard.ssl.transport.certificate_format`` config property in the Elasticsearch catalog properties file. The allowed values for this configuration are:

========================== ========================================================
Property Value	           Description
========================== ========================================================
``NONE`` (default)         Do not use Search Guard Authentication.
``PEM``                    Use X.509 PEM certificates and PKCS #8 keys.
``JKS``                    Use Keystore and Truststore files.
========================== ========================================================

If you use X.509 PEM certificates and PKCS #8 keys, the following properties must be set:

===================================================== ==============================================================================
Property Name                                         Description
===================================================== ==============================================================================
``searchguard.ssl.transport.pemcert_filepath``        Path to the X.509 node certificate chain.
``searchguard.ssl.transport.pemkey_filepath``         Path to the certificates key file.
``searchguard.ssl.transport.pemkey_password``         Key password. Omit this setting if the key has no password.
``searchguard.ssl.transport.pemtrustedcas_filepath``  Path to the root CA(s) (PEM format).
===================================================== ==============================================================================

If you use Keystore and Truststore files, the following properties must be set:

===================================================== ==============================================================================
Property Name                                         Description
===================================================== ==============================================================================
``searchguard.ssl.transport.keystore_filepath``       Path to the keystore file.
``searchguard.ssl.transport.keystore_password``       Keystore password.
``searchguard.ssl.transport.truststore_filepath``     Path to the truststore file.
``searchguard.ssl.transport.truststore_password``     Truststore password.
===================================================== ==============================================================================

``searchguard.ssl.transport.pemcert_filepath``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the X.509 node certificate chain. This file must be readable by the operating system user running Presto.

This property is optional; the default is ``etc/elasticsearch/esnode.pem``.

``searchguard.ssl.transport.pemkey_filepath``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the certificates key file. This file must be readable by the operating system user running Presto.

This property is optional; the default is ``etc/elasticsearch/esnode-key.pem``.

``searchguard.ssl.transport.pemkey_password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The key password for the key file specified by ``searchguard.ssl.transport.pemkey_filepath``.

This property is optional; the default is empty string.

``searchguard.ssl.transport.pemtrustedcas_filepath``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the root CA(s) (PEM format). This file must be readable by the operating system user running Presto.

This property is optional; the default is ``etc/elasticsearch/root-ca.pem``.

``searchguard.ssl.transport.keystore_filepath``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the keystore file. This file must be readable by the operating system user running Presto.

This property is optional; the default is ``etc/elasticsearch/keystore.jks``.

``searchguard.ssl.transport.keystore_password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The keystore password for the keystore file specified by ``searchguard.ssl.transport.keystore_filepath``

This property is optional; the default is empty string.

``searchguard.ssl.transport.truststore_filepath``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The path to the truststore file. This file must be readable by the operating system user running Presto.

This property is optional; the default is ``etc/elasticsearch/truststore.jks``.

``searchguard.ssl.transport.truststore_password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The truststore password for the truststore file specified by ``searchguard.ssl.transport.truststore_password``

This property is optional; the default is empty string.

Table Definition Files
----------------------

Elasticsearch stores the data across multiple nodes and builds indices for fast retrieval.
For Presto, this data must be mapped into columns to allow queries against the data.

A table definition file describes a table in JSON format.

.. code-block:: none

    {
        "tableName": ...,
        "schemaName": ...,
        "host": ...,
        "port": ...,
        "clusterName": ...,
        "index": ...,
        "indexExactMatch": ...,
        "type": ...
        "columns": [
            {
                "name": ...,
                "type": ...,
                "jsonPath": ...,
                "jsonType": ...,
                "ordinalPosition": ...
            }
        ]
    }

=================== ========= ============== =============================
Field               Required  Type           Description
=================== ========= ============== =============================
``tableName``       required  string         Name of the table.
``schemaName``      optional  string         Schema that contains the table. If omitted, the default schema name is used.
``host``            required  string         Elasticsearch search node host name.
``port``            required  integer        Elasticsearch search node port number.
``clusterName``     required  string         Elasticsearch cluster name.
``index``           required  string         Elasticsearch index that is backing this table.
``indexExactMatch`` optional  boolean        If set to true, the index specified with the ``index`` property is used. Otherwise, all indices starting with the prefix specified by the ``index`` property are used.
``type``            required  string         Elasticsearch `mapping type`_, which determines how the document are indexed.
``columns``         optional  list           List of column metadata information.
=================== ========= ============== =============================

.. _mapping type: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html#mapping-type

Elasticsearch Column Metadata
-----------------------------

Optionally, column metadata can be described in the same table description JSON file with these fields:

===================== ========= ============== =============================
Field                 Required  Type           Description
===================== ========= ============== =============================
``name``              optional  string         Column name of Elasticsearch field.
``type``              optional  string         Column type of Elasticsearch `field`_.
``jsonPath``          optional  string         Json path of Elasticsearch field.
``jsonType``          optional  string         Json type of Elasticsearch field.
``ordinalPosition``   optional  integer        Ordinal position of the column.
===================== ========= ============== =============================

.. _field: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
