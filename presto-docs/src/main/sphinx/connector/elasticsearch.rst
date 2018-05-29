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
    elasticsearch.table-names=schema.table1,schema.table2
    elasticsearch.default-schema=default
    elasticsearch.table-description-dir=etc/elasticsearch/
    elasticsearch.scroll-size=1000
    elasticsearch.scroll-time=60000
    elasticsearch.request-timeout=2s

Configuration Properties
------------------------

The following configuration properties are available:

======================================= ==============================================================================
Property Name                           Description
======================================= ==============================================================================
``elasticsearch.table-names``           List of tables provided by the catalog
``elasticsearch.default-schema``        Default schema name for tables
``elasticsearch.table-description-dir`` Directory containing JSON table description files
``elasticsearch.scroll-size``           Maximum number of hits to be returned with each batch of Elasticsearch scroll
``elasticsearch.scroll-timeout``        Amount of time (ms) Elasticsearch will keep the search context alive for scroll requests
``elasticsearch.max-hits``              Maximum number of hits a single Elasticsearch request can fetch
``elasticsearch.request-timeout``       Timeout for Elasticsearch requests
======================================= ==============================================================================

``elasticsearch.table-names``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of all tables provided by this catalog. A table name
can be unqualified (simple name) and will be put into the default schema
(see below) or qualified with a schema name (``<schema-name>.<table-name>``).

For each table defined here, a table description file must exist (see below).

This property is required; there is no default and at least one table must be defined.

``elasticsearch.default-schema``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema that will contain all tables defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``elasticsearch.table-description-dir``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies a path under the Presto deployment directory that contains
one or more JSON files with table descriptions (must end with ``.json``).

This property is optional; the default is ``etc/elasticsearch``.

``elasticsearch.scroll-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum number of hits that can be returned with each
batch of Elasticsearch scroll.

This property is optional; the default is ``1000``.

``elasticsearch.scroll-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the amount of time (ms) Elasticsearch will keep the `search context alive`_ for scroll requests

This property is optional; the default is ``20s``.

.. _search context alive: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#scroll-search-context

``elasticsearch.max-hits``
^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum number of `hits`_ an Elasticsearch request can fetch.

This property is optional; the default is ``1000000``.

.. _hits: https://www.elastic.co/guide/en/elasticsearch/reference/current/_the_search_api.html

``elasticsearch.request-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the timeout value for all Elasticsearch requests.

This property is optional; the default is ``10s``.

Table Definition Files
----------------------

For Presto, Elasticsearch data must be mapped to columns/tables for Presto to run queries against them.

A table definition file describes a table in JSON format.

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
``tableName``       required  string         Name of the table.
``schemaName``      optional  string         Schema that contains the table. If omitted, the default schema name is used.
``host``            required  string         Elasticsearch search node host name.
``port``            required  integer        Elasticsearch search node port number.
``clusterName``     required  string         Elasticsearch cluster name.
``index``           required  string         Elasticsearch index that is backing this table.
``indexExactMatch`` optional  boolean        true requires index name exact match, false indicates sending request to all indices with the same prefix.
``type``            required  string         Elasticsearch `mapping type`_, which determines how the document will be indexed.
``columns``         optional  list           List of column metadata information.
=================== ========= ============== =============================

.. _mapping type: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html#mapping-type

Elasticsearch Column Metadata
-----------------------------

Optionally, column metadata can be described in the same table description JSON file with these fields:

===================== ========= ============== =============================
Field                 Required  Type           Description
===================== ========= ============== =============================
``name``              optional  string         Column name of Elasticsearch `field`_.
``type``              optional  string         Column type of Elasticsearch `field`_.
``jsonPath``          optional  string         Json path of Elasticsearch `field`_.
``jsonType``          optional  string         Json type of Elasticsearch `field`_.
``ordinalPosition``   optional  integer        Ordinal position of the column.
===================== ========= ============== =============================

.. _field: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
