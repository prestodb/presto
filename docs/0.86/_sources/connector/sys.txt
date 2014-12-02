=============
System Schema
=============

The System schema provides information and metrics about the currently
running Presto cluster. It makes this available via normal SQL queries.

Configuration
-------------

The System schema doesn't need to be configured: it is automatically
available in every catalog via a special schema named ``sys``.

.. note::

    The system schema is not currently available in a standalone catalog
    and thus it must be accessed via a separately configured catalog.
    This will be improved in a future release.

Using the System Schema
-----------------------

The System schema can be accessed from any catalog. Run the following
command from anywhere to show the available tables::

    SHOW TABLES FROM sys;

Access tables in the schema by qualifying them with the schema name::

    SELECT * FROM sys.node;

System Schema Tables
--------------------

``catalog``
^^^^^^^^^^^

The catalog table contains the list of available catalogs.

``node``
^^^^^^^^

The node table contains the list of visible nodes in the Presto
cluster along with their status.

``query``
^^^^^^^^^

The query table contains information about currently and recently
running queries on the Presto cluster. From this table you can find out
the original query text (SQL), the identity of the user who ran the query
and performance information about the query including how long the query
was queued and analyzed.

``task``
^^^^^^^^

The task table contains information about the tasks involved in a
Presto query including where they were executed and and how many rows
and bytes each task processed.
