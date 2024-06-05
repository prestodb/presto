=======================
CLP Connector
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The CLP Connector enables SQL-based querying of CLP-S archives from Presto. This document describes how to setup the
CLP Connector to run SQL queries.


Configuration
-------------

To configure the CLP connector, create a catalog properties file
``etc/catalog/clp.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=clp
    clp.archive-source=local
    clp.metadata-source=mysql
    clp.metadata-db-url=jdbc:mysql://localhost:3306
    clp.metadata-db-name=clp_db
    clp.metadata-db-user=clp_user
    clp.metadata-db-password=clp_password
    clp.metadata-table-prefix=clp_
    clp.split-source=mysql


Configuration Properties
------------------------

The following configuration properties are available:

============================================= ==============================================================================
Property Name                                 Description
============================================= ==============================================================================
``clp.archive-source``                        The source of the CLP archive.
``clp.metadata-expire-interval``              The time interval after which metadata entries are considered expired.
``clp.metadata-refresh-interval``             The frequency at which metadata is refreshed from the source.
``clp.polymorphic-type-enabled``              Enables or disables support for polymorphic types within CLP.
``clp.metadata-source``                       The source from which metadata is fetched.
``clp.metadata-db-url``                       The connection URL for the metadata database.
``clp.metadata-db-name``                      The name of the metadata database.
``clp.metadata-db-user``                      The database user with access to the metadata database.
``clp.metadata-db-password``                  The password for the metadata database user.
``clp.metadata-table-prefix``                 A prefix applied to table names in the metadata database.
``clp.split-source``                          The source of split information for query execution.
============================================= ==============================================================================

``clp.archive-source``
^^^^^^^^^^^^^^^^^^^^^^

Specifies the source of the CLP archive. Supported values include ``local`` (local storage) and ``s3`` (Amazon S3).

This property is optional. The default is ``local``.

``clp.metadata-expire-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Defines how long metadata entries remain valid before being considered expired, in seconds.

This property is optional. The default is ``600``.

``clp.metadata-refresh-interval``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Specifies how frequently metadata is refreshed from the source, in seconds. This ensures that metadata remains up to
date.

Set this to a lower value for frequently changing datasets or to a higher value to reduce load.

This property is optional. The default is ``60``.

``clp.polymorphic-type-enabled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Enables or disables support for polymorphic types in CLP, allowing the same field to have different types.
This is useful for schema-less, semi-structured data where the same field may appear with different types.
When enabled, type annotations are added to conflicting field names to distinguish between types. For example, if ``id``
column appears as both an ``int`` and ``string`` types, the connector will create two columns named ``id_bigint`` and
``id_varchar``.

Supported type annotations include ``bigint``, ``varchar``, ``double``, ``boolean``, and
``array(varchar)`` (See `Data Types`_ for details). For columns with only one type, the original column name is used.

This property is optional. The default is ``false``.

``clp.metadata-source``
^^^^^^^^^^^^^^^^^^^^^^^
Currently, the only supported source is a MySQL database, which is also used by the CLP package to store metadata.
Additional sources can be supported by implementing the ``ClpMetadataProvider`` interface.

This property is optional. The default is ``mysql``.

``clp.metadata-db-url``
^^^^^^^^^^^^^^^^^^^^^^^
The JDBC URL used to connect to the metadata database.

This property is required if ``clp.metadata-source`` is set to ``mysql``.

``clp.metadata-db-name``
^^^^^^^^^^^^^^^^^^^^^^^^

The name of the metadata database.

This option is required if ``clp.metadata-source`` is set to ``mysql`` and the database name is not specified in the URL.

``clp.metadata-db-user``
^^^^^^^^^^^^^^^^^^^^^^^^

The username used to authenticate with the metadata database.

Ensure this user has read access to the relevant metadata tables.

This option is required if ``clp.metadata-source`` is set to ``mysql``.

``clp.metadata-db-password``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The password for the user specified in ``clp.metadata-db-user``.

This option is required if ``clp.metadata-source`` is set to ``mysql``.

``clp.metadata-table-prefix``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A string prefix prepended to all metadata table names when querying the database. Useful for namespacing or avoiding
collisions.

This option is optional. The default is empty.

``clp.split-source``
^^^^^^^^^^^^^^^^^^^^

Specifies the source of split information for tables. By default, it uses the same source as the metadata with the same
connection parameters. Additional sources can be supported by implementing the ``ClpSplitProvider`` interface.

This property is optional. The default is ``mysql``.

Metadata and Split Providers
----------------------------
As mentioned earlier, the CLP connector relies on metadata and split providers to retrieve information from various
sources. By default, it uses a MySQL database for both metadata and split storage. We recommend using the CLP package
for log ingestion, which automatically populates the database with the required information. However, if you prefer to
use a different source—or the same source with a custom implementation—you can provide your own implementations of
the ``ClpMetadataProvider`` and ``ClpSplitProvider`` interfaces, and configure the connector accordingly.

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

In CLP, we have three distinct string types: ``ClpString`` (strings with whitespace), ``VarString`` (strings without
whitespace), and ``DateString`` (strings representing dates). Currently, all three are mapped to Presto's ``VARCHAR``
type.

Array Types
^^^^^^^^^^^

CLP supports two array types: ``UnstructuredArray`` and ``StructuredArray``. Unstructured arrays are stored as strings
in CLP and elements can be any type. However, in Presto arrays are homogeneous, so the elements are converted to strings
when read. ``StructuredArray`` type is not supported yet.

Object Types
^^^^^^^^^^^^
CLP stores metadata using a global schema tree structure that captures all possible fields from various log structures.
Internal nodes may represent objects containing nested fields as their children. In Presto, we map these internal object
nodes specifically to the ``ROW`` data type, including all subfields as fields within the ``ROW``.

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

Each JSON log maps to this unified ``ROW`` type, with absent fields represented as ``NULL``. Thus, the child nodes
(``ts``, ``status``, ``thread_num``, ``backtrace``) become fields within the ``ROW``, clearly reflecting the nested and
varying structures of the original JSON logs.

SQL support
-----------

The connector only provides read access to data. It does not support DDL operations, such as creating or dropping
tables. Currently, we only support one ``default`` schema.
