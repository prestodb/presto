=================
Cassandra Catalog
=================

The Cassandra catalog provides the ability to query data stored in
Cassandra.

Configuration
-------------

To configure the Cassandra connector create a properties file named
``cassandra.properties`` in the ``etc/catalog`` directory with the
following content:

.. code-block:: none
    
    connector.name=cassandra
    cassandra.contact-points=host1,host2

In a real installation of Cassandra, your Cassandra cluster will
contain one or more contact points running on remote machines. To
configure Presto to query from your Cassandra cluster you should list
at least one node in your Cassandra cluster.

.. note::

   The name of the properties file ``cassandra.properties`` is
   arbitrary. Presto will create a catalog named ``cassandra`` which
   corresponds to the name of this properties file. If you are
   connecting to more than one Cassandra cluster you can create any
   number of properties files configuring multiple instances of the
   Cassandra connector.

Configuration Properties
------------------------

The following configuration properties are available for the Cassandra connector:

``cassandra.contact-points``

    This contains a comma delimited list of hosts in a Cassandra
    cluster. The Cassandra driver will use these contact points to
    discover cluster topology.

``cassandra.native-protocol-port``

    If your Cassandra nodes are not using the default port 9142 this
    property can be used to configure a custom port.

``cassandra.limit-for-partition-key-select``

    Limit of rows to read for finding all partition keys. If a
    Cassandra table has more rows than this value, splits based on
    token ranges are used instead. This property defaults to 200.

``cassandra.max-schema-refresh-threads``

    Maximum number of schema cache refresh threads. This property
    corresponds to the maximum number of parallel requests. This
    property defaults to 10.

``cassandra.schema-cache-ttl``

    Information about a schema will be stored for this duration. This
    property defaults to 1 hour.

``cassandra.schema-refresh-interval``

    Schema information will be refreshed automatically using this
    duration. This property defaults to 2 minutes.

``cassandra.consistency-level``

    Consistency levels in Cassandra refer to the level of consistency
    to be used for both read and write operations.  More information
    about consistency levels can be found in the `Cassandra
    documentation
    <http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html>`. This
    property defaults to a consistency level of ``ONE``. Possible
    values include ``ALL``, ``EACH_QUORUM``, ``QUORUM``,
    ``LOCAL_QUORUM``, ``ONE``, ``TWO``, ``THREE``, ``LOCAL_ONE``,
    ``ANY``, ``SERIAL``, ``LOCAL_SERIAL``.

``cassandra.fetch-size``

    Number of rows fetched at a time in a Cassandra query. This
    property defaults to 5000.

``cassandra.fetch-size-for-partition-key-select``

    Number of rows fetched at a time in a Cassandra query for a
    partition key. This property defaults to 20000.

``cassandra.partition-size-for-batch-select``

    This property defaults to 100.

``cassandra.thrift-port``

    Sets the port used to connect to Thrift. This property defaults to
    9160.

``cassandra.split-size``

    Sets the split size to be used when querying Cassandra data. This
    property defaults to 1024.

``cassandra.partitioner``

    Sets the partitioner to use for hashing and data
    distribution. This property defaults to ``Murmur3Partitioner``.

``cassandra.thrift-connection-factory-class``

    Allows for the specification of a custom implementation of
    ``org.apache.cassandra.thrift.ITransportFactory`` to be used to
    connect to Cassandra using the Thrift protocol. This property has
    a default value of
    ``org.apache.cassandra.thrift.TFramedTransportFactory``.

``cassandra.transport-factory-options``

    Allows for the specification of arbitrary options to be passed to
    an implementation of
    ``org.apache.cassandra.thrift.ITransportFactory`` when connecting
    to Cassandra via the Thrift protocol.

``cassandra.allow-drop-table``

    Set to ``true`` if Cassandra tables can be dropped from
    Presto. Set to ``false`` to prevent ``DROP TABLE`` queries from
    affecting Cassandra.

``cassandra.username``

    Username used for authentication to a Cassandra cluster.

``cassandra.password``

    Password used for authentication to a Cassandra cluster.

``cassandra.client.read-timeout``

    Sets the number of milliseconds the Cassandra driver will wait for
    an answer to a query from one Cassandra node. Note that the
    underlying Cassandra driver may retry a query against more than
    one node in the event of a read timeout. This property has a
    default value of 12000.

``cassandra.client.connect-timeout``

    Sets the connection timeout in milliseconds. The connection
    timeout is the number milliseconds the Cassandra driver will wait
    to establish a connection to a Cassandra node. This property has a
    default value of 5000.

``cassandra.client.so-linger``

    Sets the TCP SO_LINGER option or the linger-on-close
    timeout.. When linger is set to zero a socket will be closed
    immediately on close(). When this option is non-zero, a socket
    will linger n number of seconds for an acknowledgement that all
    data was written to a peer. This option can be used to avoid
    consuming sockets on a Cassandra server by immediately closing
    connections when they are no longer needed. This property is
    configured in a unit of seconds.


Using the Cassandra Catalog
---------------------------

To access the Cassandra catalog configured from a properties file
named ``cassandra.properties`` execute the ``use catalog`` command
shown below.

.. code-block:: none

    use catalog cassandra

Running ``show schemas`` will show you every schema configured in
Cassandra configured for this catalog.

Describing Cassandra Tables
---------------------------

The following table is an example Cassandra table from the `Getting Started guide <https://wiki.apache.org/cassandra/GettingStarted>`
of the Cassandra Wiki. If the table ``users`` is created in the Cassandra keyspace ``mykeyspace``
using the following commands in cqlsh:

.. code-block:: none

    cqlsh> CREATE KEYSPACE mykeyspace
       ... WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    cqlsh> use mykeyspace;
    cqlsh:mykeyspace> CREATE TABLE users (
                  ...   user_id int PRIMARY KEY,
                  ...   fname text,
                  ...   lname text
                  ... );


This table can be described in Presto by executing the following
commands:

.. code-block:: none

    presto:jmx> use catalog cassandra;
    presto:jmx> use schema mykeyspace;
    presto:mykeyspace> describe users;
     Column  |  Type   | Null | Partition Key | Comment 
    ---------+---------+------+---------------+---------
     user_id | bigint  | true | true          |         
     fname   | varchar | true | false         |         
     lname   | varchar | true | false         |         
    (3 rows)

This example demonstrates that Cassandra tables are available in Presto.