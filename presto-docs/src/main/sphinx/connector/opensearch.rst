====================
OpenSearch Connector
====================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The OpenSearch Connector allows access to OpenSearch data from Presto.
This document describes how to setup the OpenSearch Connector to run SQL queries against OpenSearch.

.. note::

    OpenSearch 1.0.0 or later is required.

Configuration
-------------

To configure the OpenSearch connector, create a catalog properties file
``etc/catalog/opensearch.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=opensearch
    opensearch.host=localhost
    opensearch.port=9200
    opensearch.scheme=https
    opensearch.auth.username=admin
    opensearch.auth.password=admin

Configuration Properties
------------------------

Connection Properties
^^^^^^^^^^^^^^^^^^^^^

================================================= ==============================================================================
Property Name                                     Description
================================================= ==============================================================================
``opensearch.host``                               Host name of the OpenSearch server.
``opensearch.port``                               Port of the OpenSearch server. Default is ``9200``.
``opensearch.scheme``                             Connection scheme (http or https). Default is ``https``.
``opensearch.max-connections``                    Maximum number of HTTP connections. Default is ``100``.
``opensearch.connection-timeout``                 Connection timeout in milliseconds. Default is ``10000``.
``opensearch.socket-timeout``                     Socket timeout in milliseconds. Default is ``60000``.
================================================= ==============================================================================

Authentication Properties
^^^^^^^^^^^^^^^^^^^^^^^^^

================================================= ==============================================================================
Property Name                                     Description
================================================= ==============================================================================
``opensearch.auth.username``                      Username for basic authentication.
``opensearch.auth.password``                      Password for basic authentication.
================================================= ==============================================================================

SSL/TLS Properties
^^^^^^^^^^^^^^^^^^

========================================================= ============================================================================================
Property Name                                             Description
========================================================= ============================================================================================
``opensearch.ssl.enabled``                                Enable SSL/TLS connections. Default is ``true``.
``opensearch.ssl.verify-hostname``                        Verify SSL hostname. Default is ``true``.
``opensearch.ssl.skip-certificate-validation``            Skip SSL certificate validation (including expiration checks).
                                                          Use only for testing with expired or self-signed certificates.
                                                          Default is ``false``.
``opensearch.ssl.truststore.path``                        Path to SSL truststore file (optional when using valid certificates).
``opensearch.ssl.truststore.password``                    Password for SSL truststore.
========================================================= ============================================================================================

.. warning::

    The ``opensearch.ssl.skip-certificate-validation`` property should only be used in testing or development
    environments with expired or self-signed certificates. In production, always use valid certificates and
    keep this property set to ``false`` (default).

Query Properties
^^^^^^^^^^^^^^^^

================================================= ==============================================================================
Property Name                                     Description
================================================= ==============================================================================
``opensearch.scroll.size``                        Maximum number of hits per scroll request. Default is ``1000``.
``opensearch.scroll.timeout``                     Search context timeout for scroll requests. Default is ``5m``.
``opensearch.max-result-window``                  Maximum result window size. Default is ``10000``.
================================================= ==============================================================================

Schema Discovery Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

================================================= ==============================================================================
Property Name                                     Description
================================================= ==============================================================================
``opensearch.schema-pattern``                     Pattern for discovering indices. Default is ``*``.
``opensearch.hide-system-indices``                Hide system indices (starting with ``.``). Default is ``true``.
``opensearch.hide-document-id-column``            Hide the ``_id`` column from table schema. Default is ``false``.
================================================= ==============================================================================

Vector Search Properties
^^^^^^^^^^^^^^^^^^^^^^^^

================================================= ==============================================================================
Property Name                                     Description
================================================= ==============================================================================
``opensearch.vector-search.enabled``              Enable vector search support. Default is ``true``.
``opensearch.vector-search.default-k``            Default number of nearest neighbors. Default is ``10``.
``opensearch.vector-search.default-space-type``   Default distance metric (cosine, l2, inner_product). Default is ``cosine``.
``opensearch.vector-search.default-ef-search``    Default ef_search parameter for HNSW. Default is ``100``.
================================================= ==============================================================================

Nested Field Properties
^^^^^^^^^^^^^^^^^^^^^^^

================================================= ==============================================================================
Property Name                                     Description
================================================= ==============================================================================
``opensearch.nested.enabled``                     Enable nested field access with dot notation. Default is ``true``.
``opensearch.nested.max-depth``                   Maximum depth for nested field discovery. Default is ``5``.
``opensearch.nested.optimize-queries``            Optimize nested queries by grouping predicates. Default is ``true``.
``opensearch.nested.discover-dynamic-fields``     Discover dynamic fields not in mapping. Default is ``false``.
``opensearch.nested.log-missing-fields``          Log warnings for missing nested fields. Default is ``false``.
================================================= ==============================================================================

Data Types
----------

The data type mappings are as follows:

============= =============
OpenSearch    Presto
============= =============
``binary``    ``VARBINARY``
``boolean``   ``BOOLEAN``
``byte``      ``TINYINT``
``short``     ``SMALLINT``
``integer``   ``INTEGER``
``long``      ``BIGINT``
``float``     ``REAL``
``half_float````REAL``
``double``    ``DOUBLE``
``scaled_float``  ``DOUBLE``
``text``      ``VARCHAR``
``keyword``   ``VARCHAR``
``ip``        ``VARCHAR``
``date``      ``DATE``
``knn_vector````ARRAY(REAL)``
``nested``    ``VARCHAR``
``object``    ``VARCHAR``
============= =============

.. note::

    The ``knn_vector`` type is mapped to ``ARRAY(REAL)`` to support vector similarity search operations.
    Nested and object types are currently mapped to ``VARCHAR`` containing JSON strings.

Special Columns
---------------

The following hidden columns are available:

======= =======================================================
Column  Description
======= =======================================================
_id     The OpenSearch document ID
_score  The document score returned by the OpenSearch query
_source The source of the original document
======= =======================================================

Querying OpenSearch
-------------------

The OpenSearch connector provides a schema for every OpenSearch index. You can see the available schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM opensearch;

If you have an OpenSearch index named ``products``, you can view the table in the ``default`` schema::

    DESCRIBE opensearch.default.products;
    SHOW COLUMNS FROM opensearch.default.products;

Basic Queries
^^^^^^^^^^^^^

You can query the ``products`` table::

    SELECT * FROM opensearch.default.products;

    SELECT _id, name, price
    FROM opensearch.default.products
    WHERE price > 100
    ORDER BY price DESC
    LIMIT 10;

Parallel Query Execution
^^^^^^^^^^^^^^^^^^^^^^^^^

The connector automatically handles multi-shard OpenSearch indices with proper shard routing and parallel execution. The split manager creates one split per shard, allowing Presto workers to read data from multiple shards concurrently.

Each split uses OpenSearch's ``preference`` parameter with ``_shards:N`` syntax to route queries to specific shards, ensuring correct results without data duplication. This enables efficient distributed query execution for large-scale deployments with proper load distribution across shards.

For k-NN vector searches using the table function, a single split is created that queries all shards globally, since k-NN search must aggregate results across the entire index to find the true nearest neighbors.

Nested Field Access
-------------------

The connector supports querying nested JSON structures using dot notation. Nested fields are automatically discovered from OpenSearch index mappings and exposed as virtual columns.

Example with nested fields::

    SELECT _id, user.name, user.profile.age, metadata.tags
    FROM opensearch.default.users
    WHERE user.profile.age > 25;

Configuration for nested fields:

- ``opensearch.nested.enabled`` - Enable nested field access (default: ``true``)
- ``opensearch.nested.max-depth`` - Maximum nesting depth to discover (default: ``5``)
- ``opensearch.nested.optimize-queries`` - Group predicates on same parent (default: ``true``)

Vector Search
-------------

The OpenSearch connector supports vector similarity search using the OpenSearch k-NN plugin. Vector fields with type ``knn_vector`` are automatically discovered and exposed as ``ARRAY(REAL)`` columns.

Prerequisites
^^^^^^^^^^^^^

Your OpenSearch cluster must have the k-NN plugin installed and indices must be configured with ``knn_vector`` fields.

Querying Vector Fields
^^^^^^^^^^^^^^^^^^^^^^^

Vector fields can be queried like regular array columns::

    -- View vector field
    SELECT id, title, embedding
    FROM opensearch.default.documents
    LIMIT 10;

    -- Check vector dimension
    SELECT id, cardinality(embedding) as dimension
    FROM opensearch.default.documents
    LIMIT 1;

k-NN Table Function
^^^^^^^^^^^^^^^^^^^^

The connector provides a table function for executing k-NN vector searches directly in SQL::

    SELECT * FROM TABLE(opensearch.system.knn_search(
      index_name => 'documents',
      vector_field => 'embedding',
      query_vector => ARRAY[0.1, 0.2, 0.3],
      k => 10
    ))
    ORDER BY _score DESC;

Table Function Parameters
""""""""""""""""""""""""""

=================== ============ ======== ============ =====================================================
Parameter           Type         Required Default      Description
=================== ============ ======== ============ =====================================================
``index_name``      VARCHAR      Yes      -            Name of the OpenSearch index to search
``vector_field``    VARCHAR      Yes      -            Name of the knn_vector field in the index
``query_vector``    ARRAY(REAL)  Yes      -            Query vector for similarity search
``k``               INTEGER      No       10           Number of nearest neighbors to return
``space_type``      VARCHAR      No       'cosine'     Distance metric: 'cosine', 'l2', or 'inner_product'
``ef_search``       INTEGER      No       100          HNSW ef_search parameter for accuracy/speed tradeoff
=================== ============ ======== ============ =====================================================

The table function returns columns from the index plus ``_id`` and ``_score`` columns.

Examples
""""""""

Basic k-NN search::

    SELECT _id, title, category, _score
    FROM TABLE(opensearch.system.knn_search(
      index_name => 'products',
      vector_field => 'embedding',
      query_vector => ARRAY[0.1, 0.2, 0.3],
      k => 5
    ))
    ORDER BY _score DESC;

Using custom distance metric::

    SELECT _id, title, _score
    FROM TABLE(opensearch.system.knn_search(
      index_name => 'documents',
      vector_field => 'embedding',
      query_vector => ARRAY[0.5, 0.5, 0.0],
      k => 10,
      space_type => 'l2',
      ef_search => 200
    ))
    ORDER BY _score ASC;

Filtering k-NN results::

    SELECT _id, title, category
    FROM TABLE(opensearch.system.knn_search(
      index_name => 'products',
      vector_field => 'embedding',
      query_vector => ARRAY[0.2, 0.3, 0.4],
      k => 50
    ))
    WHERE category = 'electronics'
    ORDER BY _score DESC
    LIMIT 10;

Joining with other tables::

    SELECT p.product_id, p.name, k._score
    FROM products p
    JOIN TABLE(opensearch.system.knn_search(
      index_name => 'product_vectors',
      vector_field => 'embedding',
      query_vector => ARRAY[0.1, 0.2, 0.3],
      k => 20
    )) k ON p.product_id = k._id
    WHERE k._score > 0.8
    ORDER BY k._score DESC;

The connector creates splits based on OpenSearch shards for parallel execution. For k-NN searches using the table function, a single split queries all shards globally since k-NN search must be executed across the entire index.

Limitations
-----------

The following SQL statements are not supported:

* ``DELETE``
* ``INSERT``
* ``CREATE TABLE``
* ``DROP TABLE``
* ``ALTER TABLE``

The connector is read-only and does not support write operations.

Array Types
^^^^^^^^^^^

OpenSearch does not have a dedicated array type. Fields can contain zero or more values. The connector automatically detects ``knn_vector`` fields and maps them to ``ARRAY(REAL)``. Other array fields are not automatically detected and will be treated as their base type.
