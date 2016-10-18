========================
Kafka Connector Tutorial
========================

.. contents::
    :local:
    :backlinks: none
    :depth: 2

Introduction
============

The Kafka Connector for Presto allows access to live topic data from
Apache Kafka using Presto. This tutorial shows how to set up topics and
how to create the topic description files that back Presto tables.

Installation
============

This tutorial assumes familiarity with Presto and a working local Presto
installation (see :doc:`/installation/deployment`). It will focus on
setting up Apache Kafka and integrating it with Presto.

Step 1: Install Apache Kafka
----------------------------

Download and extract `Apache Kafka <https://kafka.apache.org/>`_.

.. note::

    This tutorial was tested with Apache Kafka 0.8.1.
    It should work with any 0.8.x version of Apache Kafka.

Start ZooKeeper and the Kafka server:

.. code-block:: none

    $ bin/zookeeper-server-start.sh config/zookeeper.properties
    [2013-04-22 15:01:37,495] INFO Reading configuration from: config/zookeeper.properties (org.apache.zookeeper.server.quorum.QuorumPeerConfig)
    ...

.. code-block:: none

    $ bin/kafka-server-start.sh config/server.properties
    [2013-04-22 15:01:47,028] INFO Verifying properties (kafka.utils.VerifiableProperties)
    [2013-04-22 15:01:47,051] INFO Property socket.send.buffer.bytes is overridden to 1048576 (kafka.utils.VerifiableProperties)
    ...

This will start Zookeeper on port ``2181`` and Kafka on port ``9092``.

Step 2: Load data
-----------------

Download the tpch-kafka loader from Maven central:

.. code-block:: none

    $ curl -o kafka-tpch https://repo1.maven.org/maven2/de/softwareforge/kafka_tpch_0811/1.0/kafka_tpch_0811-1.0.sh
    $ chmod 755 kafka-tpch

Now run the ``kafka-tpch`` program to preload a number of topics with tpch data:

.. code-block:: none

    $ ./kafka-tpch load --brokers localhost:9092 --prefix tpch. --tpch-type tiny
    2014-07-28T17:17:07.594-0700     INFO    main    io.airlift.log.Logging    Logging to stderr
    2014-07-28T17:17:07.623-0700     INFO    main    de.softwareforge.kafka.LoadCommand    Processing tables: [customer, orders, lineitem, part, partsupp, supplier, nation, region]
    2014-07-28T17:17:07.981-0700     INFO    pool-1-thread-1    de.softwareforge.kafka.LoadCommand    Loading table 'customer' into topic 'tpch.customer'...
    2014-07-28T17:17:07.981-0700     INFO    pool-1-thread-2    de.softwareforge.kafka.LoadCommand    Loading table 'orders' into topic 'tpch.orders'...
    2014-07-28T17:17:07.981-0700     INFO    pool-1-thread-3    de.softwareforge.kafka.LoadCommand    Loading table 'lineitem' into topic 'tpch.lineitem'...
    2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-4    de.softwareforge.kafka.LoadCommand    Loading table 'part' into topic 'tpch.part'...
    2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-5    de.softwareforge.kafka.LoadCommand    Loading table 'partsupp' into topic 'tpch.partsupp'...
    2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-6    de.softwareforge.kafka.LoadCommand    Loading table 'supplier' into topic 'tpch.supplier'...
    2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-7    de.softwareforge.kafka.LoadCommand    Loading table 'nation' into topic 'tpch.nation'...
    2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-8    de.softwareforge.kafka.LoadCommand    Loading table 'region' into topic 'tpch.region'...
    2014-07-28T17:17:10.612-0700    ERROR    pool-1-thread-8    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.region
    2014-07-28T17:17:10.781-0700     INFO    pool-1-thread-8    de.softwareforge.kafka.LoadCommand    Generated 5 rows for table 'region'.
    2014-07-28T17:17:10.797-0700    ERROR    pool-1-thread-3    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.lineitem
    2014-07-28T17:17:10.932-0700    ERROR    pool-1-thread-1    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.customer
    2014-07-28T17:17:11.068-0700    ERROR    pool-1-thread-2    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.orders
    2014-07-28T17:17:11.200-0700    ERROR    pool-1-thread-6    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.supplier
    2014-07-28T17:17:11.319-0700     INFO    pool-1-thread-6    de.softwareforge.kafka.LoadCommand    Generated 100 rows for table 'supplier'.
    2014-07-28T17:17:11.333-0700    ERROR    pool-1-thread-4    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.part
    2014-07-28T17:17:11.466-0700    ERROR    pool-1-thread-5    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.partsupp
    2014-07-28T17:17:11.597-0700    ERROR    pool-1-thread-7    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.nation
    2014-07-28T17:17:11.706-0700     INFO    pool-1-thread-7    de.softwareforge.kafka.LoadCommand    Generated 25 rows for table 'nation'.
    2014-07-28T17:17:12.180-0700     INFO    pool-1-thread-1    de.softwareforge.kafka.LoadCommand    Generated 1500 rows for table 'customer'.
    2014-07-28T17:17:12.251-0700     INFO    pool-1-thread-4    de.softwareforge.kafka.LoadCommand    Generated 2000 rows for table 'part'.
    2014-07-28T17:17:12.905-0700     INFO    pool-1-thread-2    de.softwareforge.kafka.LoadCommand    Generated 15000 rows for table 'orders'.
    2014-07-28T17:17:12.919-0700     INFO    pool-1-thread-5    de.softwareforge.kafka.LoadCommand    Generated 8000 rows for table 'partsupp'.
    2014-07-28T17:17:13.877-0700     INFO    pool-1-thread-3    de.softwareforge.kafka.LoadCommand    Generated 60175 rows for table 'lineitem'.

Kafka now has a number of topics that are preloaded with data to query.

Step 3: Make the Kafka topics known to Presto
---------------------------------------------

In your Presto installation, add a catalog properties file
``etc/catalog/kafka.properties`` for the Kafka connector.
This file lists the Kafka nodes and topics:

.. code-block:: none

    connector.name=kafka
    kafka.nodes=localhost:9092
    kafka.table-names=tpch.customer,tpch.orders,tpch.lineitem,tpch.part,tpch.partsupp,tpch.supplier,tpch.nation,tpch.region
    kafka.hide-internal-columns=false

Now start Presto:

.. code-block:: none

    $ bin/launcher start

Because the Kafka tables all have the ``tpch.`` prefix in the configuration,
the tables are in the ``tpch`` schema. The connector is mounted into the
``kafka`` catalog because the properties file is named ``kafka.properties``.

Start the :doc:`Presto CLI </installation/cli>`:

.. code-block:: none

    $ ./presto --catalog kafka --schema tpch

List the tables to verify that things are working:

.. code-block:: none

    presto:tpch> SHOW TABLES;
      Table
    ----------
     customer
     lineitem
     nation
     orders
     part
     partsupp
     region
     supplier
    (8 rows)

Step 4: Basic data querying
---------------------------

Kafka data is unstructured and it has no metadata to describe the format of
the messages. Without further configuration, the Kafka connector can access
the data and map it in raw form but there are no actual columns besides the
built-in ones:

.. code-block:: none

    presto:tpch> DESCRIBE customer;
          Column       |  Type   | Null | Partition Key |                   Comment
    -------------------+---------+------+---------------+---------------------------------------------
     _partition_id     | bigint  | true | false         | Partition Id
     _partition_offset | bigint  | true | false         | Offset for the message within the partition
     _segment_start    | bigint  | true | false         | Segment start offset
     _segment_end      | bigint  | true | false         | Segment end offset
     _segment_count    | bigint  | true | false         | Running message count per segment
     _key              | varchar | true | false         | Key text
     _key_corrupt      | boolean | true | false         | Key data is corrupt
     _key_length       | bigint  | true | false         | Total number of key bytes
     _message          | varchar | true | false         | Message text
     _message_corrupt  | boolean | true | false         | Message data is corrupt
     _message_length   | bigint  | true | false         | Total number of message bytes
    (11 rows)

    presto:tpch> SELECT count(*) FROM customer;
     _col0
    -------
      1500

    presto:tpch> SELECT _message FROM customer LIMIT 5;
                                                                                                                                                     _message
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     {"rowNumber":1,"customerKey":1,"name":"Customer#000000001","address":"IVhzIApeRb ot,c,E","nationKey":15,"phone":"25-989-741-2988","accountBalance":711.56,"marketSegment":"BUILDING","comment":"to the even, regular platelets. regular, ironic epitaphs nag e"}
     {"rowNumber":3,"customerKey":3,"name":"Customer#000000003","address":"MG9kdTD2WBHm","nationKey":1,"phone":"11-719-748-3364","accountBalance":7498.12,"marketSegment":"AUTOMOBILE","comment":" deposits eat slyly ironic, even instructions. express foxes detect slyly. blithel
     {"rowNumber":5,"customerKey":5,"name":"Customer#000000005","address":"KvpyuHCplrB84WgAiGV6sYpZq7Tj","nationKey":3,"phone":"13-750-942-6364","accountBalance":794.47,"marketSegment":"HOUSEHOLD","comment":"n accounts will have to unwind. foxes cajole accor"}
     {"rowNumber":7,"customerKey":7,"name":"Customer#000000007","address":"TcGe5gaZNgVePxU5kRrvXBfkasDTea","nationKey":18,"phone":"28-190-982-9759","accountBalance":9561.95,"marketSegment":"AUTOMOBILE","comment":"ainst the ironic, express theodolites. express, even pinto bean
     {"rowNumber":9,"customerKey":9,"name":"Customer#000000009","address":"xKiAFTjUsCuxfeleNqefumTrjS","nationKey":8,"phone":"18-338-906-3675","accountBalance":8324.07,"marketSegment":"FURNITURE","comment":"r theodolites according to the requests wake thinly excuses: pending
    (5 rows)

    presto:tpch> SELECT sum(cast(json_extract_scalar(_message, '$.accountBalance') AS double)) FROM customer LIMIT 10;
       _col0
    ------------
     6681865.59
    (1 row)

The data from Kafka can be queried using Presto but it is not yet in
actual table shape. The raw data is available through the ``_message`` and
``_key`` columns but it is not decoded into columns. As the sample data is
in JSON format, the :doc:`/functions/json` built into Presto can be used
to slice the data.

Step 5: Add a topic decription file
-----------------------------------

The Kafka connector supports topic description files to turn raw data into
table format. These files are located in the ``etc/kafka`` folder in the
Presto installation and must end with ``.json``. It is recommended that
the file name matches the table name but this is not necessary.

Add the following file as ``etc/kafka/tpch.customer.json`` and restart Presto:

.. code-block:: json

    {
        "tableName": "customer",
        "schemaName": "tpch",
        "topicName": "tpch.customer",
        "key": {
            "dataFormat": "raw",
            "fields": [
                {
                    "name": "kafka_key",
                    "dataFormat": "LONG",
                    "type": "BIGINT",
                    "hidden": "false"
                }
            ]
        }
    }

The customer table now has an additional column: ``kafka_key``.

.. code-block:: none

    presto:tpch> DESCRIBE customer;
          Column       |  Type   | Null | Partition Key |                   Comment
    -------------------+---------+------+---------------+---------------------------------------------
     kafka_key         | bigint  | true | false         |
     _partition_id     | bigint  | true | false         | Partition Id
     _partition_offset | bigint  | true | false         | Offset for the message within the partition
     _segment_start    | bigint  | true | false         | Segment start offset
     _segment_end      | bigint  | true | false         | Segment end offset
     _segment_count    | bigint  | true | false         | Running message count per segment
     _key              | varchar | true | false         | Key text
     _key_corrupt      | boolean | true | false         | Key data is corrupt
     _key_length       | bigint  | true | false         | Total number of key bytes
     _message          | varchar | true | false         | Message text
     _message_corrupt  | boolean | true | false         | Message data is corrupt
     _message_length   | bigint  | true | false         | Total number of message bytes
    (12 rows)

    presto:tpch> SELECT kafka_key FROM customer ORDER BY kafka_key LIMIT 10;
     kafka_key
    -----------
             0
             1
             2
             3
             4
             5
             6
             7
             8
             9
    (10 rows)

The topic definition file maps the internal Kafka key (which is a raw long
in eight bytes) onto a Presto ``BIGINT`` column.

Step 6: Map all the values from the topic message onto columns
--------------------------------------------------------------

Update the ``etc/kafka/tpch.customer.json`` file to add fields for the
message and restart Presto. As the fields in the message are JSON, it uses
the ``json`` data format. This is an example where different data formats
are used for the key and the message.

.. code-block:: json

    {
        "tableName": "customer",
        "schemaName": "tpch",
        "topicName": "tpch.customer",
        "key": {
            "dataFormat": "raw",
            "fields": [
                {
                    "name": "kafka_key",
                    "dataFormat": "LONG",
                    "type": "BIGINT",
                    "hidden": "false"
                }
            ]
        },
        "message": {
            "dataFormat": "json",
            "fields": [
                {
                    "name": "row_number",
                    "mapping": "rowNumber",
                    "type": "BIGINT"
                },
                {
                    "name": "customer_key",
                    "mapping": "customerKey",
                    "type": "BIGINT"
                },
                {
                    "name": "name",
                    "mapping": "name",
                    "type": "VARCHAR"
                },
                {
                    "name": "address",
                    "mapping": "address",
                    "type": "VARCHAR"
                },
                {
                    "name": "nation_key",
                    "mapping": "nationKey",
                    "type": "BIGINT"
                },
                {
                    "name": "phone",
                    "mapping": "phone",
                    "type": "VARCHAR"
                },
                {
                    "name": "account_balance",
                    "mapping": "accountBalance",
                    "type": "DOUBLE"
                },
                {
                    "name": "market_segment",
                    "mapping": "marketSegment",
                    "type": "VARCHAR"
                },
                {
                    "name": "comment",
                    "mapping": "comment",
                    "type": "VARCHAR"
                }
            ]
        }
    }

Now for all the fields in the JSON of the message, columns are defined and
the sum query from earlier can operate on the ``account_balance`` column directly:

.. code-block:: none

    presto:tpch> DESCRIBE customer;
          Column       |  Type   | Null | Partition Key |                   Comment
    -------------------+---------+------+---------------+---------------------------------------------
     kafka_key         | bigint  | true | false         |
     row_number        | bigint  | true | false         |
     customer_key      | bigint  | true | false         |
     name              | varchar | true | false         |
     address           | varchar | true | false         |
     nation_key        | bigint  | true | false         |
     phone             | varchar | true | false         |
     account_balance   | double  | true | false         |
     market_segment    | varchar | true | false         |
     comment           | varchar | true | false         |
     _partition_id     | bigint  | true | false         | Partition Id
     _partition_offset | bigint  | true | false         | Offset for the message within the partition
     _segment_start    | bigint  | true | false         | Segment start offset
     _segment_end      | bigint  | true | false         | Segment end offset
     _segment_count    | bigint  | true | false         | Running message count per segment
     _key              | varchar | true | false         | Key text
     _key_corrupt      | boolean | true | false         | Key data is corrupt
     _key_length       | bigint  | true | false         | Total number of key bytes
     _message          | varchar | true | false         | Message text
     _message_corrupt  | boolean | true | false         | Message data is corrupt
     _message_length   | bigint  | true | false         | Total number of message bytes
    (21 rows)

    presto:tpch> SELECT * FROM customer LIMIT 5;
     kafka_key | row_number | customer_key |        name        |                address                | nation_key |      phone      | account_balance | market_segment |                                                      comment
    -----------+------------+--------------+--------------------+---------------------------------------+------------+-----------------+-----------------+----------------+---------------------------------------------------------------------------------------------------------
             1 |          2 |            2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        |         13 | 23-768-687-3665 |          121.65 | AUTOMOBILE     | l accounts. blithely ironic theodolites integrate boldly: caref
             3 |          4 |            4 | Customer#000000004 | XxVSJsLAGtn                           |          4 | 14-128-190-5944 |         2866.83 | MACHINERY      |  requests. final, regular ideas sleep final accou
             5 |          6 |            6 | Customer#000000006 | sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn  |         20 | 30-114-968-4951 |         7638.57 | AUTOMOBILE     | tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious
             7 |          8 |            8 | Customer#000000008 | I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5 |         17 | 27-147-574-9335 |         6819.74 | BUILDING       | among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly alon
             9 |         10 |           10 | Customer#000000010 | 6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2    |          5 | 15-741-346-9870 |         2753.54 | HOUSEHOLD      | es regular deposits haggle. fur
    (5 rows)

    presto:tpch> SELECT sum(account_balance) FROM customer LIMIT 10;
       _col0
    ------------
     6681865.59
    (1 row)

Now all the fields from the ``customer`` topic messages are available as
Presto table columns.

Step 7: Use live data
---------------------

Presto can query live data in Kafka as it arrives. To simulate a live feed
of data, this tutorial sets up a feed of live tweets into Kafka.

Setup a live Twitter feed
^^^^^^^^^^^^^^^^^^^^^^^^^

* Download the twistr tool

.. code-block:: none

    $ curl -o twistr https://repo1.maven.org/maven2/de/softwareforge/twistr_kafka_0811/1.2/twistr_kafka_0811-1.2.sh
    $ chmod 755 twistr

* Create a developer account at https://dev.twitter.com/ and set up an
  access and consumer token.

* Create a ``twistr.properties`` file and put the access and consumer key
  and secrets into it:

.. code-block:: none

    twistr.access-token-key=...
    twistr.access-token-secret=...
    twistr.consumer-key=...
    twistr.consumer-secret=...
    twistr.kafka.brokers=localhost:9092

Create a tweets table on Presto
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add the tweets table to the ``etc/catalog/kafka.properties`` file:

.. code-block:: none

    connector.name=kafka
    kafka.nodes=localhost:9092
    kafka.table-names=tpch.customer,tpch.orders,tpch.lineitem,tpch.part,tpch.partsupp,tpch.supplier,tpch.nation,tpch.region,tweets
    kafka.hide-internal-columns=false

Add a topic definition file for the Twitter feed as ``etc/kafka/tweets.json``:

.. code-block:: json

    {
        "tableName": "tweets",
        "topicName": "twitter_feed",
        "dataFormat": "json",
        "key": {
            "dataFormat": "raw",
            "fields": [
                {
                    "name": "kafka_key",
                    "dataFormat": "LONG",
                    "type": "BIGINT",
                    "hidden": "false"
                }
            ]
        },
        "message": {
            "dataFormat":"json",
            "fields": [
                {
                    "name": "text",
                    "mapping": "text",
                    "type": "VARCHAR"
                },
                {
                    "name": "user_name",
                    "mapping": "user/screen_name",
                    "type": "VARCHAR"
                },
                {
                    "name": "lang",
                    "mapping": "lang",
                    "type": "VARCHAR"
                },
                {
                    "name": "created_at",
                    "mapping": "created_at",
                    "type": "TIMESTAMP",
                    "dataFormat": "rfc2822"
                },
                {
                    "name": "favorite_count",
                    "mapping": "favorite_count",
                    "type": "BIGINT"
                },
                {
                    "name": "retweet_count",
                    "mapping": "retweet_count",
                    "type": "BIGINT"
                },
                {
                    "name": "favorited",
                    "mapping": "favorited",
                        "type": "BOOLEAN"
                },
                {
                    "name": "id",
                    "mapping": "id_str",
                    "type": "VARCHAR"
                },
                {
                    "name": "in_reply_to_screen_name",
                    "mapping": "in_reply_to_screen_name",
                    "type": "VARCHAR"
                },
                {
                    "name": "place_name",
                    "mapping": "place/full_name",
                    "type": "VARCHAR"
                }
            ]
        }
    }

As this table does not have an explicit schema name, it will be placed
into the ``default`` schema.

Feed live data
^^^^^^^^^^^^^^

Start the twistr tool:

.. code-block:: none

    $ java -Dness.config.location=file:$(pwd) -Dness.config=twistr -jar ./twistr

``twistr`` connects to the Twitter API and feeds the "sample tweet" feed
into a Kafka topic called ``twitter_feed``.

Now run queries against live data:

.. code-block:: none

    $ ./presto-cli --catalog kafka --schema default

    presto:default> SELECT count(*) FROM tweets;
     _col0
    -------
      4467
    (1 row)

    presto:default> SELECT count(*) FROM tweets;
     _col0
    -------
      4517
    (1 row)

    presto:default> SELECT count(*) FROM tweets;
     _col0
    -------
      4572
    (1 row)

    presto:default> SELECT kafka_key, user_name, lang, created_at FROM tweets LIMIT 10;
         kafka_key      |    user_name    | lang |       created_at
    --------------------+-----------------+------+-------------------------
     494227746231685121 | burncaniff      | en   | 2014-07-29 14:07:31.000
     494227746214535169 | gu8tn           | ja   | 2014-07-29 14:07:31.000
     494227746219126785 | pequitamedicen  | es   | 2014-07-29 14:07:31.000
     494227746201931777 | josnyS          | ht   | 2014-07-29 14:07:31.000
     494227746219110401 | Cafe510         | en   | 2014-07-29 14:07:31.000
     494227746210332673 | Da_JuanAnd_Only | en   | 2014-07-29 14:07:31.000
     494227746193956865 | Smile_Kidrauhl6 | pt   | 2014-07-29 14:07:31.000
     494227750426017793 | CashforeverCD   | en   | 2014-07-29 14:07:32.000
     494227750396653569 | FilmArsivimiz   | tr   | 2014-07-29 14:07:32.000
     494227750388256769 | jmolas          | es   | 2014-07-29 14:07:32.000
    (10 rows)

There is now a live feed into Kafka which can be queried using Presto.

Epilogue: Time stamps
---------------------

The tweets feed that was set up in the last step contains a time stamp in
RFC 2822 format as ``created_at`` attribute in each tweet.

.. code-block:: none

    presto:default> SELECT DISTINCT json_extract_scalar(_message, '$.created_at')) AS raw_date
                 -> FROM tweets LIMIT 5;
                raw_date
    --------------------------------
     Tue Jul 29 21:07:31 +0000 2014
     Tue Jul 29 21:07:32 +0000 2014
     Tue Jul 29 21:07:33 +0000 2014
     Tue Jul 29 21:07:34 +0000 2014
     Tue Jul 29 21:07:35 +0000 2014
    (5 rows)

The topic definition file for the tweets table contains a mapping onto a
timestamp using the ``rfc2822`` converter:

.. code-block:: none

    ...
    {
        "name": "created_at",
        "mapping": "created_at",
        "type": "TIMESTAMP",
        "dataFormat": "rfc2822"
    },
    ...

This allows the raw data to be mapped onto a Presto timestamp column:

.. code-block:: none

    presto:default> SELECT created_at, raw_date FROM (
                 ->   SELECT created_at, json_extract_scalar(_message, '$.created_at') AS raw_date
                 ->   FROM tweets)
                 -> GROUP BY 1, 2 LIMIT 5;
           created_at        |            raw_date
    -------------------------+--------------------------------
     2014-07-29 14:07:20.000 | Tue Jul 29 21:07:20 +0000 2014
     2014-07-29 14:07:21.000 | Tue Jul 29 21:07:21 +0000 2014
     2014-07-29 14:07:22.000 | Tue Jul 29 21:07:22 +0000 2014
     2014-07-29 14:07:23.000 | Tue Jul 29 21:07:23 +0000 2014
     2014-07-29 14:07:24.000 | Tue Jul 29 21:07:24 +0000 2014
    (5 rows)

The Kafka connector contains converters for ISO 8601, RFC 2822 text
formats and for number-based timestamps using seconds or miilliseconds
since the epoch. There is also a generic, text-based formatter which uses
Joda-Time format strings to parse text columns.
