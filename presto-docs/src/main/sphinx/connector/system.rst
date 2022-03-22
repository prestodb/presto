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

The catalogs table contains the list of available catalogs.

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
Presto query including where they were executed and and how many rows
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
