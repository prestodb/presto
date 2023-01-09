====================
Hudi connector
====================

Overview
--------

The Hudi connector enables querying `Hudi <https://hudi.apache.org/docs/overview/>`_ tables
synced to Hive metastore. The connector usesthe metastore only to track partition locations.
It makes use of the underlying Hudi filesystem and input formats to list data files. To learn
more about the design of the connector, please check out `RFC-40 <https://github.com/apache/hu
di/blob/master/rfc/rfc-44/rfc-44.md>`_.

Requirements
------------

To use Hudi, we need:

* Network access from the Presto coordinator and workers to the distributed object storage.
* Access to a Hive metastore service (HMS).
* Network access from the Presto coordinator to the HMS. Hive metastore access with the Thrift
protocol defaults to using port 9083.

Configuration
-------------

Hudi supports the same metastore configuration properties as the Hive connector. At a minimum,
following connector properties must be set in the hudi.properties file inside <presto_install_dir>
/etc/catalog directory::

    connector.name=hudi
    hive.metastore.uri=thrift://hms.host:9083

Additionally, following session properties can be set depending on the use-case.

======================================= ============================================= ===========
Property Name                           Description                                   Default
======================================= ============================================= ===========
``hudi.metadata-table-enabled``         Fetch the list of file names and sizes from   false
                                        Hudi's metadata table rather than storage.
======================================= ============================================= ===========

SQL Support
-----------

Currently, the connector only provides read access to data in the Hudi table that has been synced to
Hive metastore. Once the catalog has been configured as mentioned above, users can query the tables
as usual like Hive tables.

Supported Query Types
^^^^^^^^^^^^^^^^^^^^^

=========================== =============================================
Table Type                  Supported Query types
=========================== =============================================
Copy On Write               Snapshot Queries

Merge On Read               Snapshot Queries + Read Optimized Queries
=========================== =============================================

Examples Queries
^^^^^^^^^^^^^^^^

`stock_ticks_cow` is a Hudi cow table that we refer in the Hudi `quickstart <https://hudi.apache.org
/docs/docker_demo/>`_ document to create.

Here are some sample queries:

.. code-block:: sql

    USE hudi.default;
    select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';

.. code-block:: text

      symbol   |        _col1         |
    -----------+----------------------+
     GOOG      | 2018-08-31 10:59:00  |
    (1 rows)

.. code-block:: sql

    select dt, symbol from stock_ticks_cow where symbol = 'GOOG';

.. code-block:: text

        dt      | symbol |
    ------------+--------+
     2018-08-31 |  GOOG  |
    (1 rows)

.. code-block:: sql

    select dt, count(*) from stock_ticks_cow group by dt;

.. code-block:: text

        dt      | _col1 |
    ------------+--------+
     2018-08-31 |  99  |
    (1 rows)

