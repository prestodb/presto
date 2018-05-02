=================
Phoenix Connector
=================

The Phoenix connector allows querying data stored in Phoenix.

Compatibility
-------------

Connector is compatible with all Phoenix versions starting from 4.13.x.

Installation
------------

To install a pre-built phoenix, use these directions:

* Download and expand the latest phoenix-[version]-bin.tar.
* Add the phoenix-[version]-server.jar to the classpath of all HBase region server and master and remove any previous version. An easy way to do this is to copy it into the HBase lib directory.
* Restart HBase.
* Add Phoenix connector in Presto.

Note that this uses apache hbase version. If your HBase cluster is using CDH version,
you'll install CDH compatibility version and recompile Phoenix connector.
You can change the version by changing the `` dep.phoenix.version`` variable in pom.xml.
The Phoenix Connector provides a shaded jar to solve problems with versions of libraries
such as guava and jersey in phoenix.

Configuration
-------------

To configure the Phoenix connector, create a catalog properties file
``etc/catalog/phoenix.properties`` with the following contents,
replacing ``host1,host2,host3`` with a comma-separated list of the ZooKepper
nodes used to discovery the hbase cluster:

.. code-block:: none

    connector.name=phoenix
    connection-url=jdbc:phoenix:host1,host2,host3:2181:/hbase
    connection-properties=phoenix.schema.isNamespaceMappingEnabled=true
    allow-drop-table=true

Multiple Phoenix Clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Phoenix clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Configuration Properties
------------------------

The following configuration properties are available:

================================================== ====================== ========== ======================================================================
Property Name                                      Default Value          Required   Description
================================================== ====================== ========== ======================================================================
``connection-url``                                 (none)                 Yes        ``jdbc:phoneix[:zk_quorum][:zk_port][:zk_hbase_path]``. The ``zk_quorum`` is a comma separated list of the ZooKeeper Servers. The ``zk_port`` is the ZooKeeper port. The ``zk_hbase_path`` is HBase root znode path, that is configurable using hbase-site.xml, and by default the location is “/hbase”
``connection-properties``                          (none)                 No         Phoenix Additional connection properties like ``phoenix.schema.isNamespaceMappingEnabled``
``allow-drop-table``                               false                  No         Set to ``true`` to allow dropping Phoenix tables from Presto via :doc:`/sql/drop-table` (defaults to ``false``).
================================================== ====================== ========== ======================================================================

Querying Phoenix Tables
-------------------------

The Phoenix connector provides a schema for every Phoenix schema.
You can see the available Phoenix schemas by running ``SHOW SCHEMAS``::

    SHOW SCHEMAS FROM phoenix;

If you have a Phoenix schema named ``web``, you can view the tables
in this schema by running ``SHOW TABLES``::

    SHOW TABLES FROM phoenix.web;

You can see a list of the columns in the ``clicks`` table in the ``web`` database
using either of the following::

    DESCRIBE phoenix.web.clicks;
    SHOW COLUMNS FROM phoenix.web.clicks;

Finally, you can access the ``clicks`` table in the ``web`` schema::

    SELECT * FROM phoenix.web.clicks;

If you used a different name for your catalog properties file, use
that catalog name instead of ``phoenix`` in the above examples.

Data types
----------

The data types mappings are as follows:

==========================  ======
Phoenix                     Presto
==========================  ======
BOOLEAN                     BOOLEAN
BIGINT                      BIGINT
INTEGER                     INTEGER
SMALLINT                    SMALLINT
TINYINT                     TINYINT
DOUBLE                      DOUBLE
REAL                        FLOAT
VARBINARY                   VARBINARY
DATE                        DATE
TIME                        TIME
TIME_WITH_TIME_ZONE         TIME
TIMESTAMP                   TIMESTAMP
TIMESTAMP_WITH_TIME_ZONE    TIMESTAMP
ARRAY<?>                    ARRAY
==========================  ======

Table Properties
----------------

Table property usage example:

.. code-block:: sql

    CREATE TABLE myschema.scientists (
      recordkey VARCHAR,
      birthday DATE
      name VARCHAR,
      age BIGINT
    )
    WITH (
      rowkeys = ARRAY['recordkey', 'birthday row_timestamp'],
      salt_buckets=10
    );

=========================== ================ ======================================================================================================
Property Name               Default Value    Description
=========================== ================ ======================================================================================================
``rowkeys``                 (first column)   Presto column name that maps to the Phoenix primary key. ``row_timestamp`` is `Row timestamp <https://phoenix.apache.org/rowtimestamp.html>`
``salt_buckets``            (none)           ``salt_buckets`` numeric property causes an extra byte to be transparently prepended to every row key to ensure an evenly distributed read and write load across all region servers.
``split_on``                (none)           Per-split table Salting does automatic table splitting but in case you want to exactly control where table split occurs with out adding extra byte or change row key order then you can pre-split a table.
``disable_wal``             false            ``disable_wal`` boolean option when true causes HBase not to write data to the write-ahead-log, thus making updates faster at the expense of potentially losing data in the event of a region server failure.
``immutable_rows``          false            ``immutable_rows`` boolean option when true declares that your table has rows which are write-once, append-only (i.e. the same row is never updated).
``default_column_family``   ``0``            ``default_column_family`` string option determines the column family used used when none is specified. The value is case sensitive.
``bloomfilter``             ``ROW``          ``bloomfilter`` are enabled on a Column Family. Valid values are ``NONE``, ``ROW``(default), or ``ROWCOL``.
``versions``                ``1``            A ``{row, column, version}`` tuple exactly specifies a cell in HBase. It's possible to have an unbounded number of cells where the row and column are the same but the cell address differs only in its version dimension.
``min_versions``            ``0``            The minimum number of row versions to keep is configured per column family
``compression``             ``NONE``         HBase supports several different compression algorithms which can be enabled on a ColumnFamily. Valid values are ``NONE``(default), ``SNAPPY``, ``LZO``, ``LZ4``, or ``GZ``.
``ttl``                     ``FOREVER``      ColumnFamilies can set a TTL length in seconds, and HBase will automatically delete rows once the expiration time is reached.
=========================== ================ ======================================================================================================

Phoenix Connector Limitations
-----------------------------

* Only one dimensional arrays are currently supported.
* Does not support global and local indexes, Only rowkey based pushdown is currently supported.
