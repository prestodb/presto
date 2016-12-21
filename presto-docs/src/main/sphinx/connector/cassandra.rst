===================
Cassandra Connector
===================

The Cassandra connector allows querying data stored in Cassandra.

Configuration
-------------

To configure the Cassandra connector, create a catalog properties file
``etc/catalog/cassandra.properties`` with the following contents,
replacing ``host1,host2`` with a comma-separated list of the Cassandra
nodes used to discovery the cluster topology:

.. code-block:: none

    connector.name=cassandra
    cassandra.contact-points=host1,host2

You will also need to set ``cassandra.native-protocol-port`` if your
Cassandra nodes are not using the default port (9042).

Multiple Cassandra Clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Cassandra clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Configuration Properties
------------------------

The following configuration properties are available:

================================================== ======================================================================
Property Name                                      Description
================================================== ======================================================================
``cassandra.contact-points``                       Comma-separated list of hosts in a Cassandra cluster. The Cassandra
                                                   driver will use these contact points to discover cluster topology.
                                                   At least one Cassandra host is required.

``cassandra.native-protocol-port``                 The Cassandra server port running the native client protocol
                                                   (defaults to ``9042``).

``cassandra.thrift-port``                          The Cassandra server port running the Thrift client protocol
                                                   (defaults to ``9160``).

``cassandra.limit-for-partition-key-select``       Limit of rows to read for finding all partition keys. If a
                                                   Cassandra table has more rows than this value, splits based on
                                                   token ranges are used instead. Note that for larger values you
                                                   may need to adjust read timeout for Cassandra.

``cassandra.max-schema-refresh-threads``           Maximum number of schema cache refresh threads. This property
                                                   corresponds to the maximum number of parallel requests.

``cassandra.schema-cache-ttl``                     Maximum time that information about a schema will be cached
                                                   (defaults to ``1h``).

``cassandra.schema-refresh-interval``              The schema information cache will be refreshed in the background
                                                   when accessed if the cached data is at least this old
                                                   (defaults to ``2m``).

``cassandra.consistency-level``                    Consistency levels in Cassandra refer to the level of consistency
                                                   to be used for both read and write operations.  More information
                                                   about consistency levels can be found in the
                                                   `Cassandra consistency`_ documentation. This property defaults to
                                                   a consistency level of ``ONE``. Possible values include ``ALL``,
                                                   ``EACH_QUORUM``, ``QUORUM``, ``LOCAL_QUORUM``, ``ONE``, ``TWO``,
                                                   ``THREE``, ``LOCAL_ONE``, ``ANY``, ``SERIAL``, ``LOCAL_SERIAL``.

``cassandra.allow-drop-table``                     Set to ``true`` to allow dropping Cassandra tables from Presto
                                                   via :doc:`/sql/drop-table` (defaults to ``false``).

``cassandra.username``                             Username used for authentication to the Cassandra cluster.
                                                   This is a global setting used for all connections, regardless
                                                   of the user who is connected to Presto.

``cassandra.password``                             Password used for authentication to the Cassandra cluster.
                                                   This is a global setting used for all connections, regardless
                                                   of the user who is connected to Presto.
================================================== ======================================================================

.. _Cassandra consistency: http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html

The following advanced configuration properties are available:

============================================================= ======================================================================
Property Name                                                 Description
============================================================= ======================================================================
``cassandra.fetch-size``                                      Number of rows fetched at a time in a Cassandra query.

``cassandra.fetch-size-for-partition-key-select``             Number of rows fetched at a time in a Cassandra query that
                                                              selects partition keys.

``cassandra.partition-size-for-batch-select``                 Number of partitions batched together into a single select for a
                                                              single partion key column table.

``cassandra.split-size``                                      Number of keys per split when querying Cassandra.

``cassandra.partitioner``                                     Partitioner to use for hashing and data distribution. This
                                                              property defaults to ``Murmur3Partitioner``. The other supported
                                                              values are ``RandomPartitioner`` and ``ByteOrderedPartitioner``.

``cassandra.thrift-connection-factory-class``                 Allows for the specification of a custom implementation of
                                                              ``org.apache.cassandra.thrift.ITransportFactory`` to be used to
                                                              connect to Cassandra using the Thrift protocol.

``cassandra.transport-factory-options``                       Allows for the specification of arbitrary options to be passed to
                                                              the Thrift connection factory.

``cassandra.client.read-timeout``                             Maximum time the Cassandra driver will wait for an
                                                              answer to a query from one Cassandra node. Note that the underlying
                                                              Cassandra driver may retry a query against more than one node in
                                                              the event of a read timeout. Increasing this may help with queries
                                                              that use an index.

``cassandra.client.connect-timeout``                          Maximum time the Cassandra driver will wait to establish
                                                              a connection to a Cassandra node. Increasing this may help with
                                                              heavily loaded Cassandra clusters.

``cassandra.client.so-linger``                                Number of seconds to linger on close if unsent data is queued.
                                                              If set to zero, the socket will be closed immediately.
                                                              When this option is non-zero, a socket will linger that many
                                                              seconds for an acknowledgement that all data was written to a
                                                              peer. This option can be used to avoid consuming sockets on a
                                                              Cassandra server by immediately closing connections when they
                                                              are no longer needed.

``cassandra.retry-policy``                                    Policy used to retry failed requests to Cassandra. This property
                                                              defaults to ``DEFAULT``. Using ``BACKOFF`` may help when
                                                              queries fail with *"not enough replicas"*. The other possible
                                                              values are ``DOWNGRADING_CONSISTENCY`` and ``FALLTHROUGH``.

``cassandra.load-policy.use-dc-aware``                        Set to ``true`` to use ``DCAwareRoundRobinPolicy``
                                                              (defaults to ``false``).

``cassandra.load-policy.dc-aware.local-dc``                   The name of the local datacenter for ``DCAwareRoundRobinPolicy``.

``cassandra.load-policy.dc-aware.used-hosts-per-remote-dc``   Uses the provided number of host per remote datacenter
                                                              as failover for the local hosts for ``DCAwareRoundRobinPolicy``.

``cassandra.load-policy.dc-aware.allow-remote-dc-for-local``  Set to ``true`` to allow to use hosts of
                                                              remote datacenter for local consistency level.

``cassandra.load-policy.use-token-aware``                     Set to ``true`` to use ``TokenAwarePolicy`` (defaults to ``false``).

``cassandra.load-policy.shuffle-replicas``                    Set to ``true`` to use ``TokenAwarePolicy`` with shuffling of replicas
                                                              (defaults to ``false``).

``cassandra.load-policy.use-white-list``                      Set to ``true`` to use ``WhiteListPolicy`` (defaults to ``false``).

``cassandra.load-policy.white-list.addresses``                Comma-separated list of hosts for ``WhiteListPolicy``.

``cassandra.no-host-available-retry-count``                   Retry count for ``NoHostAvailableException`` (defaults to ``1``).

``cassandra.speculative-execution.limit``                     The number of speculative executions (defaults to ``1``).

``cassandra.speculative-execution.delay``                     The delay between each speculative execution (defaults to ``500ms``).
============================================================= ======================================================================

Querying Cassandra Tables
-------------------------

The ``users`` table is an example Cassandra table from the Cassandra
`Getting Started`_ guide. It can be created along with the ``mykeyspace``
keyspace using Cassandra's cqlsh (CQL interactive terminal):

.. _Getting Started: https://wiki.apache.org/cassandra/GettingStarted

.. code-block:: none

    cqlsh> CREATE KEYSPACE mykeyspace
       ... WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    cqlsh> USE mykeyspace;
    cqlsh:mykeyspace> CREATE TABLE users (
                  ...   user_id int PRIMARY KEY,
                  ...   fname text,
                  ...   lname text
                  ... );

This table can be described in Presto::

    DESCRIBE cassandra.mykeyspace.users;

.. code-block:: none

     Column  |  Type   | Extra | Comment
    ---------+---------+-------+---------
     user_id | bigint  |       |
     fname   | varchar |       |
     lname   | varchar |       |
    (3 rows)

This table can then be queried in Presto::

    SELECT * FROM cassandra.mykeyspace.users;
