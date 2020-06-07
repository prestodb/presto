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

    Elasticsearch 6.0.0 or later is required.

Configuration
-------------

To configure the Elasticsearch connector, create a catalog properties file
``etc/catalog/elasticsearch.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=elasticsearch
    elasticsearch.host=localhost
    elasticsearch.port=9200
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
``elasticsearch.host``                        Host name of the Elasticsearch server.
``elasticsearch.port``                        Port of the Elasticsearch server.
``elasticsearch.default-schema``              Default schema name for tables.
``elasticsearch.table-description-directory`` Directory containing JSON table description files.
``elasticsearch.scroll-size``                 Maximum number of hits to be returned with each Elasticsearch scroll request.
``elasticsearch.scroll-timeout``              Amount of time Elasticsearch will keep the search context alive for scroll requests.
``elasticsearch.max-hits``                    Maximum number of hits a single Elasticsearch request can fetch.
``elasticsearch.request-timeout``             Timeout for Elasticsearch requests.
``elasticsearch.connect-timeout``             Timeout for connections to Elasticsearch hosts.
``elasticsearch.max-request-retries``         Maximum number of Elasticsearch request retries.
``elasticsearch.max-request-retry-time``      Use exponential backoff starting at 1s up to the value specified by this configuration when retrying failed requests.
============================================= ==============================================================================

``elasticsearch.host``
^^^^^^^^^^^^^^^^^^^^^^

Specifies the hostname of the Elasticsearch node to connect to.

This property is required.

``elasticsearch.port``
^^^^^^^^^^^^^^^^^^^^^^

Specifies the port of the Elasticsearch node to connect to.

This property is optional; the default is ``9200``.

``elasticsearch.cluster-name``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the cluster name of the Elasticsearch cluster.

This property is optional; the default is ``null``.

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

``elasticsearch.connect-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the timeout value for all Elasticsearch connection attempts.

This property is optional; the default is ``1s``.

``elasticsearch.max-request-retries``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum number of Elasticsearch request retries.

This property is optional; the default is ``5``.

``elasticsearch.max-request-retry-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use exponential backoff starting at 1s up to the value specified by this configuration when retrying failed requests.

This property is optional; the default is ``10s``.

TLS Security
------------
The Elasticsearch connector provides additional security options to support Elasticsearch clusters that have been configured to use TLS.

The connector supports key stores and trust stores in PEM or Java Key Store (JKS) format. The allowed configuration values are:

===================================================== ==============================================================================
Property Name                                         Description
===================================================== ==============================================================================
``elasticsearch.tls.enabled``                         Whether TLS security is enabled.
``elasticsearch.tls.verify-hostnames``                Whether to verify Elasticsearch server hostnames.
``elasticsearch.tls.keystore-path``                   Path to the PEM or JKS key store.
``elasticsearch.tls.truststore-path``                 Path to the PEM or JKS trust store.
``elasticsearch.tls.keystore-password``               Password for the key store.
``elasticsearch.tls.truststore-password``             Password for the trust store.
===================================================== ==============================================================================

``elasticsearch.tls.keystore-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The path to the PEM or JKS key store. This file must be readable by the operating system user running Presto.

This property is optional.

``elasticsearch.tls.truststore-path``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The path to PEM or JKS trust store. This file must be readable by the operating system user running Presto.

This property is optional.

``elasticsearch.tls.keystore-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The key password for the key store specified by ``elasticsearch.tls.keystore-path``.

This property is optional.

``elasticsearch.tls.truststore-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The key password for the trust store specified by ``elasticsearch.tls.truststore-path``.

This property is optional.

Table Definition Files
----------------------

Elasticsearch stores the data across multiple nodes and builds indices for fast retrieval.
For Presto, this data must be mapped into columns to allow queries against the data.

A table definition file describes a table in JSON format.

.. code-block:: none

    {
        "tableName": ...,
        "schemaName": ...,
        "index": ...,
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
``index``           required  string         Elasticsearch index that is backing this table.
``type``            optional  string         Elasticsearch `mapping type`_, which determines how the document are indexed (like "_doc").
``columns``         optional  list           List of column metadata information.
=================== ========= ============== =============================

.. _mapping type: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html#mapping-type

Elasticsearch Column Metadata
-----------------------------

Optionally, column metadata can be described in the same table description JSON file with these fields:

===================== ========= ============== =============================
Field                 Required  Type           Description
===================== ========= ============== =============================
``name``              required  string         Column name of Elasticsearch field.
``type``              required  string         Column type of Elasticsearch `field`_ (see second column of `data type mapping`_).
``jsonPath``          required  string         Json path of Elasticsearch field (when in doubt set to the same as ``name``).
``jsonType``          required  string         Json type of Elasticsearch field (when in doubt set to the same as ``type``).
``ordinalPosition``   optional  integer        Ordinal position of the column.
===================== ========= ============== =============================

.. _field: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html

.. _data type mapping: #data-types

Data Types
----------

The data type mappings are as follows:

============= ======
Elasticsearch Presto
============= ======
``binary``    ``VARBINARY``
``boolean``   ``BOOLEAN``
``double``    ``DOUBLE``
``float``     ``DOUBLE``
``integer``   ``INTEGER``
``keyword``   ``VARCHAR``
``long``      ``BIGINT``
``string``    ``VARCHAR``
``text``      ``VARCHAR``
(others)      (unsupported)
============= ======
