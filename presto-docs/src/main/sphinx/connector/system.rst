================
System Connector
================

The System connector provides information and metrics about the currently
running Presto cluster. It makes this available via normal SQL queries.

Configuration
-------------

The System connector doesn't need to be configured: it is automatically
available via a catalog named ``system``.

Using the System Connector
--------------------------

List the available system schemas::

    SHOW SCHEMAS FROM system;

List the tables in one of the schemas::

    SHOW TABLES FROM system.runtime;

Query one of the tables::

    SELECT * FROM system.runtime.nodes;

Kill a running query::

    CALL system.runtime.kill_query(query_id => '20151207_215727_00146_tx3nr', message => 'Using too many resources');

System Connector Tables
-----------------------

``metadata.catalogs``
^^^^^^^^^^^^^^^^^^^^^

The catalogs table contains the list of available catalogs. The columns in ``metadata.catalogs`` are:

======================================= ======================================================================
Column Name                             Description
======================================= ======================================================================
``catalog_name``                        The value of this column is derived from the names of
                                        catalog.properties files present under ``etc/catalog`` path under
                                        presto installation directory. Everything except the suffix
                                        ``.properties`` is treated as the catalog name. For example, if there
                                        is a file named ``my_catalog.properties``, then ``my_catalog`` will be
                                        listed as the value for this column.

``connector_id``                        The values in this column are a duplicate of the values in the
                                        ``catalog_name`` column.

``connector_name``                      This column represents the actual name of the underlying connector
                                        that a particular catalog is using. This column contains the value of
                                        ``connector.name`` property from the catalog.properties file.
======================================= ======================================================================

Example:

Suppose a user configures a single catalog by creating a file named ``my_catalog.properties`` with the
below contents::

    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://localhost:9083

``metadata.catalogs`` table will show below output::

    presto> select * from system.metadata.catalogs;
     catalog_name | connector_id | connector_name
    --------------+--------------+----------------
     my_catalog   | my_catalog   | hive-hadoop2

``metadata.schema_properties``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The schema properties table contains the list of available properties
that can be set when creating a new schema.

``metadata.table_properties``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The table properties table contains the list of available properties
that can be set when creating a new table.

``runtime.nodes``
^^^^^^^^^^^^^^^^^

The nodes table contains the list of visible nodes in the Presto
cluster along with their status.

``runtime.queries``
^^^^^^^^^^^^^^^^^^^

The queries table contains information about currently and recently
running queries on the Presto cluster. From this table you can find out
the original query text (SQL), the identity of the user who ran the query
and performance information about the query including how long the query
was queued and analyzed.

``runtime.tasks``
^^^^^^^^^^^^^^^^^

The tasks table contains information about the tasks involved in a
Presto query including where they were executed and how many rows
and bytes each task processed.

``runtime.transactions``
^^^^^^^^^^^^^^^^^^^^^^^^

The transactions table contains the list of currently open transactions
and related metadata. This includes information such as the create time,
idle time, initialization parameters, and accessed catalogs.

System Connector Procedures
---------------------------

.. function:: runtime.kill_query(query_id, message)

    Kill the query identified by ``query_id``. The query failure message
    will include the specified ``message``.
