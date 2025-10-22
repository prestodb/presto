=================
MongoDB Connector
=================

This connector allows the use of MongoDB collections as tables in Presto.

.. note::

    MongoDB 2.6+ is supported although it is highly recommend to use 3.0 or later.

Configuration
-------------

To configure the MongoDB connector, create a catalog properties file
``etc/catalog/mongodb.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=mongodb
    mongodb.seeds=host1,host:port

Multiple MongoDB Clusters
^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
MongoDB clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Configuration Properties
------------------------

The following configuration properties are available:

===================================== ==============================================================
Property Name                         Description
===================================== ==============================================================
``mongodb.seeds``                     List of all mongod servers
``mongodb.schema-collection``         A collection which contains schema information
``mongodb.credentials``               List of credentials
``mongodb.min-connections-per-host``  The minimum size of the connection pool per host
``mongodb.connections-per-host``      The maximum size of the connection pool per host
``mongodb.max-wait-time``             The maximum wait time
``mongodb.connection-timeout``        The socket connect timeout
``mongodb.socket-timeout``            The socket timeout
``mongodb.socket-keep-alive``         Whether keep-alive is enabled on each socket
``mongodb.ssl.enabled``               Use TLS/SSL for connections to mongod/mongos
``mongodb.read-preference``           The read preference
``mongodb.write-concern``             The write concern
``mongodb.required-replica-set``      The required replica set name
``mongodb.cursor-batch-size``         The number of elements to return in a batch
``case-sensitive-name-matching``      Enable case-sensitive identifier support for schema,
                                      table, and column names for the connector. When disabled,
                                      names are matched case-insensitively using lowercase
                                      normalization. Default is ``false``
===================================== ==============================================================

``mongodb.seeds``
^^^^^^^^^^^^^^^^^

Comma-separated list of ``hostname[:port]`` all mongod servers in the same replica set or a list of mongos servers in the same sharded cluster. If port is not specified, port 27017 will be used.

This property is required; there is no default and at least one seed must be defined.

``mongodb.schema-collection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As the MongoDB is a document database, there's no fixed schema information in the system. So a special collection in each MongoDB database should defines the schema of all tables. Please refer the :ref:`table-definition-label` section for the details.

At startup, this connector tries guessing fields' types, but it might not be correct for your collection. In that case, you need to modify it manually. ``CREATE TABLE`` and ``CREATE TABLE AS SELECT`` will create an entry for you.

This property is optional; the default is ``_schema``.

``mongodb.credentials``
^^^^^^^^^^^^^^^^^^^^^^^

A comma separated list of ``username:password@collection`` credentials

This property is optional; no default value.

``mongodb.min-connections-per-host``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The minimum number of connections per host for this MongoClient instance. Those connections will be kept in a pool when idle, and the pool will ensure over time that it contains at least this minimum number.

This property is optional; the default is ``0``.

``mongodb.connections-per-host``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The maximum number of connections allowed per host for this MongoClient instance. Those connections will be kept in a pool when idle. Once the pool is exhausted, any operation requiring a connection will block waiting for an available connection.

This property is optional; the default is ``100``.

``mongodb.max-wait-time``
^^^^^^^^^^^^^^^^^^^^^^^^^

The maximum wait time in milliseconds that a thread may wait for a connection to become available.
A value of ``0`` means that it will not wait. A negative value means to wait indefinitely for a connection to become available.

This property is optional; the default is ``120000``.

``mongodb.connection-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connection timeout in milliseconds. A value of ``0`` means no timeout. It is used solely when establishing a new connection.

This property is optional; the default is ``10000``.

``mongodb.socket-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^

The socket timeout in milliseconds. It is used for I/O socket read and write operations.

This property is optional; the default is ``0`` and means no timeout.

``mongodb.socket-keep-alive``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This flag controls the socket keep alive feature that keeps a connection alive through firewalls.

This property is optional; the default is ``false``.

``mongodb.ssl.enabled``
^^^^^^^^^^^^^^^^^^^^^^^^

This flag enables SSL connections to MongoDB servers.

This property is optional and defaults to ``false``. If you set it to ``true`` and host Presto yourself, it’s likely that you also use a TLS CA file.

For setup instructions, see :ref:`tls-ca-definition-label`.

``mongodb.read-preference``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The read preference to use for queries, map-reduce, aggregation, and count.
The available values are ``PRIMARY``, ``PRIMARY_PREFERRED``, ``SECONDARY``, ``SECONDARY_PREFERRED`` and ``NEAREST``.

This property is optional; the default is ``PRIMARY``.

``mongodb.write-concern``
^^^^^^^^^^^^^^^^^^^^^^^^^

The write concern to use. The available values are
``ACKNOWLEDGED``, ``FSYNC_SAFE``, ``FSYNCED``, ``JOURNAL_SAFE``, ``JOURNALED``, ``MAJORITY``,
``NORMAL``, ``REPLICA_ACKNOWLEDGED``, ``REPLICAS_SAFE`` and ``UNACKNOWLEDGED``.

This property is optional; the default is ``ACKNOWLEDGED``.

``mongodb.required-replica-set``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The required replica set name. With this option set, the MongoClient instance will

#. Connect in replica set mode, and discover all members of the set based on the given servers
#. Make sure that the set name reported by all members matches the required set name.
#. Refuse to service any requests if any member of the seed list is not part of a replica set with the required name.

This property is optional; no default value.

``mongodb.cursor-batch-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Limits the number of elements returned in one batch. A cursor typically fetches a batch of result objects and stores them locally.
If batchSize is 0, Driver's default will be used.
If batchSize is positive, it represents the size of each batch of objects retrieved. It can be adjusted to optimize performance and limit data transfer.
If batchSize is negative, it will limit of number objects returned, that fit within the max batch size limit (usually 4MB), and cursor will be closed. For example if batchSize is -10, then the server will return a maximum of 10 documents and as many as can fit in 4MB, then close the cursor.

.. note:: Do not use a batch size of ``1``.

This property is optional; the default is ``0``.

.. _tls-ca-definition-label:

Configuring the MongoDB Connector to Use a TLS CA File
------------------------------------------------------

A TLS CA file may be required to connect securely to a MongoDB cluster hosted on DigitalOcean. MongoDB clusters are hosted on multiple nodes, each with its own hostname. Cluster hostnames do not resolve using standard ``dig`` requests to the hostname in the connection string.

Retrieve the Node Hostnames
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To retrieve the node hostnames of a cluster using ``dig``, specify the ``srv`` record type in the request and prepend ``_mongodb._tcp.`` to the hostname in the connection string, as shown below:

.. code-block:: bash

    dig srv _mongodb._tcp.<cluster-hostname>

For example, a properly formatted ``dig`` request would look like this:

.. code-block:: bash

    dig srv _mongodb._tcp.mongodb-prod-cluster-ba6e9b05.mongo.ondigitalocean.com

The ``dig`` command returns the actual hosts (in the **Answer Section**) that you can use to connect to MongoDB through Presto. The regular hostname won’t work and will result in a ``host not found`` error.

Set Up a TLS CA File
^^^^^^^^^^^^^^^^^^^^

The following steps were developed using CentOS. Adapt them as needed for your environment.

1. Create the certificate file:

   .. code-block:: bash

       touch /etc/pki/ca-trust/source/anchors/mongo.prod-cluster.crt

2. Paste the contents of the TLS CA file into the newly created file.

3. Update the trust store by running the following command:

   .. code-block:: bash

       update-ca-trust

4. Verify the setup by running the following command:

   .. code-block:: bash

       openssl s_client -connect <host-found-with-dig-above>:27017 < /dev/null

   The output should include ``CONNECTED`` and ``Verification: OK``, indicating the SSL connection is properly configured.

Configure the Catalog
^^^^^^^^^^^^^^^^^^^^^

To configure a MongoDB catalog for this cluster, follow these steps:

1. Create the catalog configuration file:

   .. code-block:: bash

       touch etc/catalog/mongodb.properties

2. Edit the file and include the host found using ``dig`` in `Retrieve the Node Hostnames <#retrieve-the-node-hostnames>`_. For example:

   .. code-block:: none

       connector.name=mongodb
       mongodb.seeds=<host-found-with-dig-above>:27017
       mongodb.credentials=<user>:<password>@<mongodb-auth-source>
       mongodb.ssl.enabled=true
       mongodb.required-replica-set=<mongodb-replica-set>

Run Queries
^^^^^^^^^^^

After starting the Presto server, you should be able to connect to the catalog and execute queries. For instance:

.. code-block:: sql

    SELECT name
    FROM users
    WHERE _id = ObjectId('66fe8898c4ce1100c811cbe0');

.. _table-definition-label:

Table Definition
----------------

MongoDB maintains table definitions on the special collection where ``mongodb.schema-collection`` configuration value specifies.

.. note::

    There's no way for the plugin to detect a collection is deleted.
    You need to delete the entry by ``db.getCollection("_schema").remove( { table: deleted_table_name })`` in the Mongo Shell.
    Or drop a collection by running ``DROP TABLE table_name`` using Presto.

A schema collection consists of a MongoDB document for a table.

.. code-block:: none

    {
        "table": ...,
        "fields": [
              { "name" : ...,
                "type" : "varchar|bigint|boolean|double|date|array(bigint)|...",
                "hidden" : false },
                ...
            ]
        }
    }

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``table``       required  string         Presto table name
``fields``      required  array          A list of field definitions. Each field definition creates a new column in the Presto table.
=============== ========= ============== =============================

Each field definition:

.. code-block:: none

    {
        "name": ...,
        "type": ...,
        "hidden": ...
    }

=============== ========= ========= =============================
Field           Required  Type      Description
=============== ========= ========= =============================
``name``        required  string    Name of the column in the Presto table.
``type``        required  string    Presto type of the column.
``hidden``      optional  boolean   Hides the column from ``DESCRIBE <table name>`` and ``SELECT *``. Defaults to ``false``.
=============== ========= ========= =============================

There is no limit on field descriptions for either key or message.

JSON Type Handling
------------------

The connector supports writing ``json`` columns by converting their contents to BSON
using ``.parse(...)``.

For example:

.. code-block:: sql

    CREATE TABLE orders (
        orderkey bigint,
        orderstatus varchar,
        totalprice double,
        orderdate date,
        metadata json
    );

    INSERT INTO orders VALUES (
        3,
        'processing',
        150.0,
        current_date,
        JSON '{"created_by": "admin", "priority": "high"}'
    );

The JSON string must be well-formed. If it's not, the insert will fail with a parsing error.

ObjectId
--------

MongoDB collection has the special field ``_id``. The connector tries to follow the same rules for this special field, so there will be hidden field ``_id``.

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS orders (
        orderkey bigint,
        orderstatus varchar,
        totalprice double,
        orderdate date
    );

    INSERT INTO orders VALUES(1, 'bad', 50.0, current_date);
    INSERT INTO orders VALUES(2, 'good', 100.0, current_date);
    SELECT _id, * FROM orders;

.. code-block:: none

                     _id                 | orderkey | orderstatus | totalprice | orderdate
    -------------------------------------+----------+-------------+------------+------------
     55 b1 51 63 38 64 d6 43 8c 61 a9 ce |        1 | bad         |       50.0 | 2015-07-23
     55 b1 51 67 38 64 d6 43 8c 61 a9 cf |        2 | good        |      100.0 | 2015-07-23
    (2 rows)

.. code-block:: sql

    SELECT _id, * FROM orders WHERE _id = ObjectId('55b151633864d6438c61a9ce');

.. code-block:: none

                     _id                 | orderkey | orderstatus | totalprice | orderdate
    -------------------------------------+----------+-------------+------------+------------
     55 b1 51 63 38 64 d6 43 8c 61 a9 ce |        1 | bad         |       50.0 | 2015-07-23
    (1 row)

.. note::

    Unfortunately, there is no way to represent ``_id`` fields more fancy like ``55b151633864d6438c61a9ce``.

SQL support
-----------

ALTER TABLE
^^^^^^^^^^^

.. code-block:: sql

    ALTER TABLE mongodb.admin.sample_table ADD COLUMN new_col INT;
    ALTER TABLE mongodb.admin.sample_table DROP COLUMN new_col;
    ALTER TABLE mongodb.admin.sample_table RENAME COLUMN is_active TO is_enabled;
    ALTER TABLE mongodb.admin.sample_table RENAME TO renamed_table;

.. note:: Presto does not support altering the data type of a column directly with the ALTER TABLE command.

 .. code-block:: sql

   ALTER TABLE mongodb.admin.users ALTER COLUMN age TYPE BIGINT;

 returns an error similar to the following:

 ``Query 20240720_123348_00014_v7vrn failed: line 1:55: mismatched input 'int'. Expecting: 'FUNCTION', 'SCHEMA', 'TABLE'``
