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
    elasticsearch.default-schema-name=default

Configuration Properties
------------------------

The following configuration properties are available:

============================================= ==============================================================================
Property Name                                 Description
============================================= ==============================================================================
``elasticsearch.host``                        Host name of the Elasticsearch server.
``elasticsearch.port``                        Port of the Elasticsearch server.
``elasticsearch.default-schema-name``         Default schema name for tables.
``elasticsearch.scroll-size``                 Maximum number of hits to be returned with each Elasticsearch scroll request.
``elasticsearch.scroll-timeout``              Amount of time Elasticsearch will keep the search context alive for scroll requests.
``elasticsearch.max-hits``                    Maximum number of hits a single Elasticsearch request can fetch.
``elasticsearch.request-timeout``             Timeout for Elasticsearch requests.
``elasticsearch.connect-timeout``             Timeout for connections to Elasticsearch hosts.
``elasticsearch.max-retry-time``              Maximum duration across all retry attempts for a single request.
``elasticsearch.node-refresh-interval``       How often to refresh the list of available Elasticsearch nodes.
``elasticsearch.max-http-connections``        Maximum number of persistent HTTP connections to Elasticsearch.
``elasticsearch.http-thread-count``           Number of threads handling HTTP connections to Elasticsearch.
``elasticsearch.ignore-publish-address``      Whether to ignore the published address and use the configured address.
============================================= ==============================================================================

``elasticsearch.host``
^^^^^^^^^^^^^^^^^^^^^^

Specifies the hostname of the Elasticsearch node to connect to.

This property is required.

``elasticsearch.port``
^^^^^^^^^^^^^^^^^^^^^^

Specifies the port of the Elasticsearch node to connect to.

This property is optional; the default is ``9200``.

``elasticsearch.default-schema-name``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema that will contain all tables defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``elasticsearch.scroll-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum number of hits that can be returned with each
Elasticsearch scroll request.

This property is optional; the default is ``1000``.

``elasticsearch.scroll-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the amount of time Elasticsearch will keep the `search context alive`_ for scroll requests

This property is optional; the default is ``1m``.

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

``elasticsearch.max-retry-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property defines the maximum duration across all retry attempts for a single request to Elasticsearch.

This property is optional; the default is ``20s``.

``elasticsearch.node-refresh-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property controls how often the list of available Elasticsearch nodes is refreshed.

This property is optional; the default is ``1m``.

``elasticsearch.max-http-connections``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property controls the maximum number of persistent HTTP connections to Elasticsearch.

This property is optional; the default is ``25``.

``elasticsearch.http-thread-count``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This property controls the number of threads handling HTTP connections to Elasticsearch.

This property is optional; the default is number of available processors.

``elasticsearch.ignore-publish-address``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The address is used to address Elasticsearch nodes. When running in a container environment, the
published address may not match the public address of the container.  This option makes the
connector ignore the published address and use the configured address, instead.

This property is optional; the default is ``false``.

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

Data Types
----------

The data type mappings are as follows:

============= =============
Elasticsearch Presto
============= =============
``binary``    ``VARBINARY``
``boolean``   ``BOOLEAN``
``double``    ``DOUBLE``
``float``     ``REAL``
``byte``      ``TINYINT``
``short``     ``SMALLINT``
``integer``   ``INTEGER``
``keyword``   ``VARCHAR``
``long``      ``BIGINT``
``text``      ``VARCHAR``
``date``      ``TIMESTAMP``
``ip``        ``IPADDRESS``
(others)      (unsupported)
============= =============


Array Types
^^^^^^^^^^^

Fields in Elasticsearch can contain `zero or more values <https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html>`_
, but there is no dedicated array type. To indicate a field contains an array, it can be annotated in a Presto-specific structure in
the `_meta <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html>`_ section of the index mapping.

For example, you can have an Elasticsearch index that contains documents with the following structure:

.. code-block:: json

    {
        "array_string_field": ["presto","is","the","besto"],
        "long_field": 314159265359,
        "id_field": "564e6982-88ee-4498-aa98-df9e3f6b6109",
        "timestamp_field": "1987-09-17T06:22:48.000Z",
        "object_field": {
            "array_int_field": [86,75,309],
            "int_field": 2
        }
    }

The array fields of this structure can be defined by using the following command to add the field
property definition to the ``_meta.presto`` property of the target index mapping.

.. code-block:: shell

    curl --request PUT \
        --url localhost:9200/doc/_mapping \
        --header 'content-type: application/json' \
        --data '
    {
        "_meta": {
            "presto":{
                "array_string_field":{
                    "isArray":true
                },
                "object_field":{
                    "array_int_field":{
                        "isArray":true
                    }
                },
            }
        }
    }'

Special Columns
---------------

The following hidden columns are available:

======= =======================================================
Column  Description
======= =======================================================
_id     The Elasticsearch document ID
_score  The document score returned by the Elasticsearch query
_source The source of the original document
======= =======================================================

Full Text Queries
-----------------

Presto SQL queries can be combined with Elasticsearch queries by providing the `full text query`_
as part of the table name, separated by a colon. For example:

.. code-block:: sql

    SELECT * FROM elasticsearch.default."tweets: +presto DB^2"

.. _full text query: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax


Pass-through Queries
--------------------

The Elasticsearch connector allows you to embed any valid Elasticsearch query,
that uses the `Elasticsearch Query DSL
<https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>`_
in your SQL query.

The results can then be used in any SQL statement, wrapping the Elasticsearch
query. The syntax extends the syntax of the enhanced Elasticsearch table names
with the following::

    SELECT * FROM es.default."<index>$query:<es-query>"

The Elasticsearch query string ``es-query`` is base32-encoded to avoid having to
deal with escaping quotes and case sensitivity issues in table identifiers.

The result of these query tables is a table with a single row and a single
column named ``result`` of type VARCHAR. It contains the JSON payload returned
by Elasticsearch, and can be processed with the :doc:`built-in JSON functions
</functions/json>`.

AWS Authorization
-----------------

To enable AWS authorization using IAM policies, the ``elasticsearch.security`` option needs to be set to ``AWS``.
Additionally, the following options need to be configured appropriately:

================================================ ==================================================================
Property Name                                    Description
================================================ ==================================================================
``elasticsearch.aws.region``                     AWS region or the Elasticsearch endpoint. This option is required.
``elasticsearch.aws.access-key``                 AWS access key to use to connect to the Elasticsearch domain.
``elasticsearch.aws.secret-key``                 AWS secret key to use to connect to the Elasticsearch domain.
``elasticsearch.aws.use-instance-credentials``   Use the EC2 metadata service to retrieve API credentials.
================================================ ==================================================================
