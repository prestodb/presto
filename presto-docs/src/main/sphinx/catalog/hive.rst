============
Hive Catalog
============

The Hive catalog provides the ability to query data stored in
Hadoop. This connector is named the Hive connector because it connects
to a Hive Metastore using the Thrift protocol.

Configuration
-------------

To configure the Hive connector create a properties file named
``hive.properties`` in the ``etc/catalog`` directory with the
following content:

.. code-block:: none
    
    connector.name=hive-hadoop2
    hive.metastore.uri=thrift://localhost:9083

Four Hive connectors providing compatibility with four different
versions of Hadoop ship with the Presto server distribution:

* Apache Hadoop 1.x via the ``hive-hadoop1`` connector
* Apache Hadoop 2.x via the ``hive-hadoop2`` connector
* Cloudera CDH 4 via the ``hive-cdh4`` connector
* Cloudera CDH 5 via the ``hive-cdh5`` connector

.. note::

   The name of the properties file ``hive.properties`` is
   arbitrary. If you name the property file ``red.properties`` Presto
   will create a catalog named ``red`` using the Hive connector. If
   you are connecting to more than one Hive metastore you can create
   any number of properties files configuring multiple instances of
   the Hive connector.

Configuration Properties
------------------------

The following configuration properties are available for the Hive
connector:

``hive.metastore.uri``

    This is the URI of the Hive Metastore to connect to. Presto
    connects to a Hive Metastore using the Thrift protocol. An example
    value of this property is ``thrift://10.65.23.3:9083``.  This
    property is required.

Using the Hive Catalog
----------------------

To access the Hive catalog configured from a properties file named
``hive.properties`` execute the ``use catalog`` command shown below.

.. code-block:: none

    use catalog hive

Running ``show schemas`` will show you every schema configured in the
Hive metastore configured for this catalog.

Describing Hive Tables
----------------------

To describe a Hive table in Presto run ``describe table`` followed by
the name of a table in Hive.

The following table is an example Hive table from the `Usage and
Examples section
<https://cwiki.apache.org/confluence/display/Hive/Tutorial#Tutorial-UsageandExamples>`
of the Hive tutorial. If the table ``page_view`` is created in Hive
using the following ``CREATE TABLE`` command:

.. code-block:: none

    hive> CREATE TABLE page_view(viewTime INT, userid BIGINT,
        >                 page_url STRING, referrer_url STRING,
        >                 ip STRING COMMENT 'IP Address of the User')
        > COMMENT 'This is the page view table'
        > PARTITIONED BY(dt STRING, country STRING)
        > STORED AS SEQUENCEFILE;
    OK
    Time taken: 3.644 seconds

Assuming that this table was created in the ``default`` schema in
Hive, this table can be described in Presto by executing the following
commands:

.. code-block:: none

    presto:default> use catalog hive;
    presto:default> use schema default;
    presto:default> describe page_view;
        Column    |  Type   | Null | Partition Key |        Comment         
    --------------+---------+------+---------------+------------------------
     viewtime     | bigint  | true | false         |                        
     userid       | bigint  | true | false         |                        
     page_url     | varchar | true | false         |                        
     referrer_url | varchar | true | false         |                        
     ip           | varchar | true | false         | IP Address of the User 
     dt           | varchar | true | true          |                        
     country      | varchar | true | true          |                        
    (7 rows)

This example demonstrates that Hive tables are available in Presto.