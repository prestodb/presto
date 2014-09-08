==============
System Schema
==============

The System schema (sys) provides information and statistics about
catalogs, queries, nodes, and tasks running in a Presto cluster.


Configuration
-------------

The System schema doesn't need to be configured, it is an implicit
catalog running in every instance of Presto. The ``sys`` schema is
present regardless of the currently selected catalog.

Using the System Schema
-----------------------

To use the System schema, run the following command.

.. code-block:: none

    use schema sys


System Schema Tables
--------------------

Running ``show tables`` against the ``sys`` schema will list four
tables:

``sys.catalog``

    The ``catalog`` table contains two columns ``catalog_name`` and
    ``catalog_id``. Use this table to find the list of configured
    catalogs.

``sys.node``

    The ``node`` table contains four columns ``node_id``,
    ``http_uri``, ``node_version``, and ``is_active``. Query this
    table to see the status of your Presto cluster and how many nodes
    are available to run queries.

``sys.query``

    The ``query`` table contains information about queries run on a
    Presto cluster.  From this table you can find out the ``node_id``,
    ``query_id``, ``state``, and ``query`` syntax used to run a
    query. You can also find out performance information about a query
    including how long the query was queued and analyzed as well as
    the id of the user who ran the query.

``sys.task``

    The ``task`` table contains information about tasks involved in a
    Presto query.  You can find out how many tasks were involved in a
    query, where they were executed, and how many rows and bytes each
    task processed.




