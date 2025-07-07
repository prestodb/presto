=============
CLP Connector
=============

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The CLP Connector enables SQL-based querying of `CLP <https://github.com/y-scope/clp>`_ archives via Presto. This
document describes how to configure the CLP Connector for use with a CLP cluster, as well as essential details for
understanding the CLP connector.


Configuration
-------------

To configure the CLP connector, create a catalog properties file ``etc/catalog/clp.properties`` with at least the
following contents, modifying the properties as appropriate:

.. code-block:: ini

    connector.name=clp
    clp.metadata-provider-type=mysql
    clp.metadata-db-url=jdbc:mysql://localhost:3306
    clp.metadata-db-name=clp_db
    clp.metadata-db-user=clp_user
    clp.metadata-db-password=clp_password
    clp.metadata-filter-config=/path/to/metadata-filter-config.json
    clp.metadata-table-prefix=clp_
    clp.split-provider-type=mysql


Configuration Properties
------------------------

The following configuration properties are available:

================================== ======================================================================== =========
Property Name                      Description                                                              Default
================================== ======================================================================== =========
``clp.polymorphic-type-enabled``   Enables or disables support for polymorphic types in CLP, allowing the   ``false``
                                   same field to have different types. This is useful for schema-less,
                                   semi-structured data where the same field may appear with different
                                   types.

                                   When enabled, type annotations are added to conflicting field names to
                                   distinguish between types. For example, if an ``id`` column appears with
                                   both an ``int`` and ``string`` types, the connector will create two
                                   columns named ``id_bigint`` and ``id_varchar``.

                                   Supported type annotations include ``bigint``, ``varchar``, ``double``,
                                   ``boolean``, and ``array(varchar)`` (See `Data Types`_ for details). For
                                   columns with only one type, the original column name is used.
``clp.metadata-provider-type``     Specifies the metadata provider type. Currently, the only supported      ``mysql``
                                   type is a MySQL database, which is also used by the CLP package to store
                                   metadata. Additional providers can be supported by implementing the
                                   ``ClpMetadataProvider`` interface.
``clp.metadata-db-url``            The JDBC URL used to connect to the metadata database. This property is
                                   required if ``clp.metadata-provider-type`` is set to ``mysql``.
``clp.metadata-db-name``           The name of the metadata database. This option is required if
                                   ``clp.metadata-provider-type`` is set to ``mysql`` and the database name
                                   is not specified in the URL.
``clp.metadata-db-user``           The database user with access to the metadata database. This option is
                                   required if ``clp.metadata-provider-type`` is set to ``mysql`` and the
                                   database name is not specified in the URL.
``clp.metadata-db-password``       The password for the metadata database user. This option is required if
                                   ``clp.metadata-provider-type`` is set to ``mysql``.
``clp.metadata-filter-config``     The absolute path of the metadata filter config file.
``clp.metadata-table-prefix``      A string prefix prepended to all metadata table names when querying the
                                   database. Useful for namespacing or avoiding collisions. This option is
                                   required if ``clp.metadata-provider-type`` is set to ``mysql``.
``clp.metadata-expire-interval``   Defines how long, in seconds, metadata entries remain valid before they  600
                                   need to be refreshed.
``clp.metadata-refresh-interval``  Specifies how frequently metadata is refreshed from the source, in       60
                                   seconds. Set this to a lower value for frequently changing datasets or
                                   to a higher value to reduce load.
``clp.split-provider-type``        Specifies the split provider type. By default, it uses the same type as  ``mysql``
                                   the metadata provider with the same connection parameters. Additional
                                   types can be supported by implementing the ``ClpSplitProvider``
                                   interface.
================================== ======================================================================== =========


Metadata and Split Providers
----------------------------

The CLP connector relies on metadata and split providers to retrieve information from various sources. By default, it
uses a MySQL database for both metadata and split storage. We recommend using the CLP package for log ingestion, which
automatically populates the database with the required information.

If you prefer to use a different source--or the same source with a custom implementation--you can provide your own
implementations of the ``ClpMetadataProvider`` and ``ClpSplitProvider`` interfaces, and configure the connector
accordingly.

Metadata Filter Config File
----------------------------

The metadata filter config file allows you to configure the set of columns that can be used to filter out irrelevant
splits (CLP archives) when querying CLP's metadata database. This can significantly improve performance by reducing the
amount of data that needs to be scanned. For a given query, the connector will translate any supported filter predicates
that involve the configured columns into a query against CLP's metadata database.

The configuration is a JSON object where each key under the root represents a :ref:`scope<scopes>` and each scope maps
to an array of :ref:`filter configs<filter-configs>`.


.. _scopes:

Scopes
^^^^^^

A *scope* can be one of the following:

- A catalog name
- A fully-qualified schema name
- A fully-qualified table name

Filter configs under a particular scope will apply to all child scopes. For example, filter configs at the schema level
will apply to all tables within that schema.

.. _filter-configs:

Filter Configs
^^^^^^^^^^^^^^

Each `filter config` indicates how a *data column*---a column in the Presto table---should be mapped to a *metadata
column*---a column in CLP's metadata database. In most cases, the data column and the metadata column will have the same
name; but in some cases, the data column may be remapped.

For example, an integer data column (e.g., ``timestamp``), may be remapped to a pair of metadata columns that represent
the range of possible values (e.g., ``begin_timestamp`` and ``end_timestamp``) of the data column within a split.

Each *filter config* has the following properties:

- ``columnName``: The data column's name.

  .. note:: Currently, only numeric-type columns can be used as metadata filters.

- ``rangeMapping`` *(optional)*: an object with the following properties:

  .. note:: This option is only valid if the column has a numeric type.

  - ``lowerBound``: The metadata column that represents the lower bound of values in a split for the data column.
  - ``upperBound``: The metadata column that represents the upper bound of values in a split for the data column.


- ``required`` *(optional, defaults to false)*: indicates whether the filter **must** be present in the translated
  metadata filter SQL query. If a required filter is missing or cannot be pushed down, the query will be rejected.


Example
^^^^^^^

The code block shows an example metadata filter config file:

.. code-block:: json

    {
      "clp": [
        {
          "columnName": "level"
        }
      ],
      "clp.default": [
        {
          "columnName": "author"
        }
      ],
      "clp.default.table_1": [
        {
          "columnName": "msg.timestamp",
          "rangeMapping": {
            "lowerBound": "begin_timestamp",
            "upperBound": "end_timestamp"
          },
          "required": true
        },
        {
          "columnName": "file_name"
        }
      ]
    }

- The first key-value pair adds the following filter configs for all schemas and tables under the ``clp`` catalog:

  - The column ``level`` is used as-is without remapping.

- The second key-value pair adds the following filter configs for all tables under the ``clp.default`` schema:

  - The column ``author`` is used as-is without remapping.

- The third key-value pair adds two filter configs for the table ``clp.default.table_1``:

  - The column ``msg.timestamp`` is remapped via a ``rangeMapping`` to the metadata columns ``begin_timestamp`` and
    ``end_timestamp``, and is required to exist in every query.
  - The column ``file_name`` is used as-is without remapping.

Supported SQL Expressions
^^^^^^^^^^^^^^^^^^^^^^^^^

The connector supports translations from a Presto SQL query to the metadata filter query for the following expressions:

- Comparisons between variables and constants (e.g., ``=``, ``!=``, ``<``, ``>``, ``<=``, ``>=``).
- Dereferencing fields from row-typed variables.
- Logical operators: ``AND``, ``OR``, and ``NOT``.

Data Types
----------

The data type mappings are as follows:

====================== ====================
CLP Type               Presto Type
====================== ====================
``Integer``            ``BIGINT``
``Float``              ``DOUBLE``
``ClpString``          ``VARCHAR``
``VarString``          ``VARCHAR``
``DateString``         ``VARCHAR``
``Boolean``            ``BOOLEAN``
``UnstructuredArray``  ``ARRAY(VARCHAR)``
``Object``             ``ROW``
(others)               (unsupported)
====================== ====================

String Types
^^^^^^^^^^^^

CLP uses three distinct string types: ``ClpString`` (strings with whitespace), ``VarString`` (strings without
whitespace), and ``DateString`` (strings representing dates). Currently, all three are mapped to Presto's ``VARCHAR``
type.

Array Types
^^^^^^^^^^^

CLP supports two array types: ``UnstructuredArray`` and ``StructuredArray``. Unstructured arrays are stored as strings
in CLP and elements can be any type. However, in Presto arrays are homogeneous, so the elements are converted to strings
when read. ``StructuredArray`` type is not supported in Presto.

Object Types
^^^^^^^^^^^^

CLP stores metadata using a global schema tree structure that captures all possible fields from various log structures.
Internal nodes may represent objects containing nested fields as their children. In Presto, we map these internal object
nodes to the ``ROW`` data type, including all subfields as fields within the ``ROW``.

For instance, consider a table containing two distinct JSON log types:

Log Type 1:

.. code-block:: json

   {
     "msg": {
       "ts": 0,
       "status": "ok"
     }
   }

Log Type 2:

.. code-block:: json

   {
     "msg": {
       "ts": 1,
       "status": "error",
       "thread_num": 4,
       "backtrace": ""
     }
   }

In CLP's schema tree, these two structures are combined into a unified internal node (``msg``) with four child nodes:
``ts``, ``status``, ``thread_num`` and ``backtrace``. In Presto, we represent this combined structure using the
following ``ROW`` type:

.. code-block:: sql

   ROW(ts BIGINT, status VARCHAR, thread_num BIGINT, backtrace VARCHAR)

Each JSON log maps to this unified ``ROW`` type, with absent fields represented as ``NULL``. The child nodes (``ts``,
``status``, ``thread_num``, ``backtrace``) become fields within the ``ROW``, clearly reflecting the nested and varying
structures of the original JSON logs.

SQL support
-----------

The connector only provides read access to data. It does not support DDL operations, such as creating or dropping
tables. Currently, we only support one ``default`` schema.
