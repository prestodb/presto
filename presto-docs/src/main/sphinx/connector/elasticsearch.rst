=======================
Elasticsearch Connector
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The Elasticsearch Connector allows access to Elasticsearch data from Presto. This document describes how to setup the Elasticsearch Connector to run SQL on Elasticsearch.

.. note::

    It is highly recommended to use Elasticsearch 6.0.0 or later.

Configuration
-------------

To configure the Elasticsearch connector, create a catalog properties file
``etc/catalog/elasticsearch.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=elasticsearch
    elasticsearch.table-names=schema.table1,schema.table2
    elasticsearch.default-schema=default
    elasticsearch.table-description-dir=etc/elasticsearch/
    elasticsearch.scroll-size=1000
    elasticsearch.scroll-time=60000

Configuration Properties
------------------------

The following configuration properties are available:

======================================= ==============================================================================
Property Name                           Description
======================================= ==============================================================================
``elasticsearch.table-names``           List of all tables provided by the catalog
``elasticsearch.default-schema``        Default schema name for tables
``elasticsearch.table-description-dir`` Directory containing JSON table description files
``elasticsearch.scroll-size``           Maximum number of hits to be returned with each batch of Elasticsearch scroll
``elasticsearch.scroll-timeout``        Number of milliseconds Elasticsearch would keep live for a search scroll result
``elasticsearch.max-hits``              Maximum number of hits a single Elasticsearch request could fetch
``elasticsearch.request-timeout``       Elasticsearch request timeout
======================================= ==============================================================================

``elasticsearch.table-names``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of all tables provided by this catalog. A table name
can be unqualified (simple name) and will be put into the default schema
(see below) or qualified with a schema name (``<schema-name>.<table-name>``).

For each table defined here, a table description file (see below) must exist.

This property is required; there is no default and at least one table must be defined.

``elasticsearch.default-schema``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema which will contain all tables that were defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``elasticsearch.table-description-dir``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

References a directory in the Presto deployment directory that contains
one or more JSON files with table descriptions (must end with ``.json``).

This property is optional; the default is ``etc/elasticsearch``.

``elasticsearch.scroll-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Presto connects to Elasticsearch using an Elasticsearch client, the client will
use scroll and fetch to get data from Elasticsearch. This property defines the maximum
number of hits to be returned with each batch of Elasticsearch scroll.

This property is optional; the default is ``1000``.

``elasticsearch.scroll-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Presto connects to Elasticsearch using an Elasticsearch client, the client will
use scroll and fetch to get data from Elasticsearch. This property defines number of
milliseconds the Elasticsearch client would keep live for a search scroll result.

This property is optional; the default is ``20s``.

``elasticsearch.max-hits``
^^^^^^^^^^^^^^^^^^^^^^^^^^

When Presto connects to Elasticsearch using an Elasticsearch client, the client will
send request to and get data from Elasticsearch. This property defines maximum number
of hits an Elasticsearch request can fetch.

This property is optional; the default is ``1000000``.

``elasticsearch.request-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Presto connects to Elasticsearch using an Elasticsearch client, the client will
send request to fetch data from Elasticsearch. This property defines timeout
for all Elasticsearch request.

This property is optional; the default is ``10s``.

Table Definition Files
----------------------

Elasticsearch maintains documents and indexes in a highly scalable way. It provides
full-text search and analytics capabilities. For Presto, Elasticsearch data must be
mapped to tables and columns to allow queries against the data.

A table definition file consists of a JSON definition for a table. The
name of the file can be arbitrary, but it must end with ``.json``.

.. code-block:: none

    {
        "tableName": ...,
        "schemaName": ...,
        "hostAddress": ...,
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
``tableName``       required  string         Presto table name defined by this file.
``schemaName``      optional  string         Schema that contains the table. If omitted, the default schema name is used.
``hostAddress``     required  string         Elasticsearch search node host address.
``port``            required  string         Elasticsearch search node port number.
``clusterName``     required  string         Elasticsearch cluster name.
``index``           required  string         Elasticsearch index that is backing this table.
``indexExactMatch`` optional  boolean        true requires index name exact match, false indicates sending request to all indices with the same prefix.
``type``            required  string         Elasticsearch type, which represents a group of similar documents.
``columns``         optional  list           List of column metadata information.
=================== ========= ============== =============================

Elasticsearch Column Metadata
-----------------------------

Optionally, Elasticsearch column metadata can be configured in data definition files.

===================== ========= ============== =============================
Field                 Required  Type           Description
===================== ========= ============== =============================
``name``              optional  string         Column name of Elasticsearch index field.
``type``              optional  string         Column type of Elasticsearch index field. 
``jsonPath``          optional  string         Json path of Elasticsearch index field.
``jsonType``          optional  string         Json type of Elasticsearch index field.
``ordinalPosition``   optional  integer        Ordinal position of the column.
===================== ========= ============== =============================
