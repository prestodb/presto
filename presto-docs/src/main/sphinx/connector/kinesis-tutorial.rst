===========================
Kinesis Connector Tutorial
===========================

Introduction
============

The Kinesis connector for Presto allows access to data from live
Kinesis streams, fetch it and process the data on user query. This
tutorial shows how to set up Kinesis streams, how to create table
description files that back presto tables.

Set Up
======

This tutorial assumes that you already have AWS use account and have
access to Aamazon Kinesis through access keys of root accout or IAM
roles. It will focus on how to creating streams and pushing in twitter
data, and then how to query the data in Presto.

..note::
    If using terminal, enter access key id and secret key in ~/.aws/credentials file.
    Load script can read access keys from this file to load and fetch data from Kinesis.

For the purpose of showing how to use data from Kinesis stream, this
tutorial use twitter data and push it into kinesis streams

Step 1: Create Kinesis Stream
-----------------------------

Create a stream using aws-kinesis cli command ``create-stream``. It is assumed that
aws credentials are saved in ~/.aws/credentials file. (The tutorial is creating a
'twitter_data' stream with 4 shards to put twitter feeds)

..code-block:: create_stream.py

    aws kinesis create-stream --stream-name twitter_data --shard-count 4

Step 2: Setup a live twiter feed to Kinesis Stream
--------------------------------------------------

Download loadTweet.py script-

..code-block:: create_stream.py

    $ curl -o loadTweets.py https://raw.githubusercontent.com/shubham166/kinesis-load-tweets/master/loadTweets.py

* Create a developer account at https://dev.twitter.com/ and set up an
  access and consumer token.
  
* Create a ``twitter.properties`` file and put the access and consumer key
  and secrets into it:

..code-block:: create_stream.py

    api_key = <your-api-key>
    api_secret = <your-api-secret-key>
    access_token_key = <your-access-token>
    access_token_secret = <your-secret-access-token>

Step 3: Create twitter_data table in Presto
-------------------------------------------

In your Presto Installation, add a catalog properties file
``etc/catalog/kinesis.properties`` for the Kinesis connector.
This file lists all the Kinesis streams and access and secret
key to be used to access them.

.. code-block:: none

    connector.name=kinesis
    kinesis.table-names=twitter_data
    kinesis.hide-internal-columns-hidden=false
    kinesis.access-key=<your-kinesis-access-key>
    kinesis.secret-key=<your-kinesis-secret-key>

Now start Presto:

.. code-block:: none

    $ bin/launcher start

Start the :doc:`Presto CLI </installation/cli>`:

.. code-block:: none

    $ ./presto --catalog kinesis

List the tables to verify that things are working:

.. code-block:: none

    presto:default> SHOW TABLES;
      Table
    ------------
    twitter_data
    (1 row)

Step 4 : Feed live tweets to the Stream
---------------------------------------

Run the loadTweets.py script with required parameters

.. code-block:: none

    $ python loadPython.py <stream-name> <aws-access-key(optional)> <aws-secret-key(optional)>
 
'stream-name' parameter is required while aws-credentials parameters are optional.

* If AWS credentials are already stored in ~/.aws/credentials file, use

.. code-block:: none

    $ python loadPython.py twitter_data

* If credentials not present in ~/.aws/credentials file or want to overwrite with new credentials, use

.. code-block:: none

    $ python loadPython.py twitter_data <aws-access-key> <aws-secret-key>

Step 5: Basic data querying
---------------------------

Kinesis data is unstructured and it has no metadata to describe the format of
the messages. Without further configuration, the Kinesis connector can access
the data and map it in raw form but there are no actual columns besides the
built-one one:

.. code-block:: none

    presto:default> DESCRIBE twitter_data;
          Column       |  Type   | Null | Partition Key |                   Comment
    -------------------+---------+------+---------------+---------------------------------------------
    _shard_id          | varchar | true | false         | Shard Id
    _shard_sequence_id | varchar | true | false         | sequence id of messages within the shard
    _segment_start     | varchar | true | false         | segment start sequence id
    _segment_end       | varchar | true | false         | segment end sequence id
    _segment_count     | bigint  | true | false         | Running message coutn per segment
    _partition_key     | bigint  | true | false         | Key text
    _message           | varchar | true | false         | Message text
    _message_valid     | boolean | true | false         | Message data is valid
    _message_length    | bigint  | true | false         | Total number of message bytes
    (9 rows)

    presto:default> SELECT count(*) FROM twitter_data;
     _col0
    -------
      1500

      presto:default> SELECT _message FROM twitter_data LIMIT 5;
                                                                                                                                                       _message
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    {"created_at":"Mon Jun 01 12:13:24 +0000 2015","id":605346403448160256,"id_str":"605346403448160256","text":"the programming talent myth: https:\/\/t.co\/4IqNkyaI2Q\nmuch good sense here, well-stated. Even if i only agree with half","so
    {"created_at":"Mon Jun 01 12:14:41 +0000 2015","id":605346728431091712,"id_str":"605346728431091712","text":"RT @CompSciFact: Functional Programming Fundamentals. 13 lectures by @headinthebox http:\/\/t.co\/fHFwSK9uLk","source":"\u003ca
    {"created_at":"Mon Jun 01 12:15:14 +0000 2015","id":605346866595803136,"id_str":"605346866595803136","text":"#ConciergeChoice:  This Festival offers bold &amp; innovative programming in dance &amp; theatre #TRANSAM\u0083RIQUES http:\/\/
    {"created_at":"Mon Jun 01 12:16:23 +0000 2015","id":605347157097476096,"id_str":"605347157097476096","text":"RT @stefstivala: 'the most exciting part of computer programming' \ud83d\ude36\ud83d\udd2b http:\/\/t.co\/7iWHkKq68k","source":
    {"created_at":"Mon Jun 01 12:17:37 +0000 2015","id":605347466121216000,"id_str":"605347466121216000","text":"tonybaroneee comments on \"The programming talent myth\" - http:\/\/t.co\/eQlYnFyyLU","source":"\u003ca href=\"http:\/\/runwher

The data from Kinesis streams can be queried using Presto but it is not yet in
actual table shape. The raw data is available through the ``_message``columns
but it is not decoded into columns. As the sample data is in JSON format, the
:doc:`/functions/json` built into Presto can be used to slice the data.

Step 5: Add a topic decription file
-----------------------------------

The Kinesis connector supports topic description files to turn raw data into
table format. These files are located in the ``etc/kinesis`` folder in the
Presto installation and must end with ``.json``. It is recommended that
the file name matches the table name but this is not necessary.

Add the following file as ``etc/kinesis/twitter_data.json`` and restart Presto.

.. code-block:: json

    {
        "tableName": "twitter_data",
        "schemaName": "default",
        "streamName": "twitter_data",
        "message": {
            "dataFormat": "json",
            "fields": [
                {
                    "name": "created_at",
                    "mapping": "created_at",
                    "type": "TIMESTAMP",
                    "dataFormat": "rfc2822"
                },
                {
                    "name": "id",
                    "mapping": "id",
                    "type": "BIGINT"
                },
                {
                    "name": "name",
                    "mapping": "user/screen_name",
                    "type": "VARCHAR"
                },
                {
                    "name": "location",
                    "mapping": "user/location",
                    "type": "VARCHAR"
                },
                {
                    "name": "tweet",
                    "mapping": "text",
                    "type": "VARCHAR"
                },
                {
                    "name": "hashtag",
                    "mapping": "entities/hashtags",
                    "type": "VARCHAR"
                }
            ]
        }
    }

Now the table twitter_data has additional columns:

.. code-block:: none

     presto:default> DESCRIBE twitter_data;
          Column       |  Type   | Null | Partition Key |                   Comment
    -------------------+---------+------+---------------+---------------------------------------------
    create_at          |timestamp| true | false         |
    id                 | bigint  | true | false         |
    name               | varchar | true | false         |
    location           | varchar | true | false         |
    hashtag            | varchar | true | false         |
    _shard_id          | varchar | true | false         | Shard Id
    _shard_sequence_id | varchar | true | false         | sequence id of messages within the shard
    _segment_start     | varchar | true | false         | segment start sequence id
    _segment_end       | varchar | true | false         | segment end sequence id
    _segment_count     | bigint  | true | false         | Running message coutn per segment
    _partition_key     | bigint  | true | false         | Key text
    _message           | varchar | true | false         | Message text
    _message_valid     | boolean | true | false         | Message data is valid
    _message_length    | bigint  | true | false         | Total number of message bytes
    (15 rows)
    
    presto:default> select created_at, id, name, location, tweet, hashtag from twitter_data limit 5;
               created_at    |         id         |    name     |  location   |                                             tweet                                             | hashtag 
    -------------------------+--------------------+-------------+-------------+-----------------------------------------------------------------------------------------------+---------
     2015-06-01 08:12:37.000 | 605346208438202368 | sheyi6002   | Dreamland   | What are you terrible at? — maths and programming xD. i dunno http://t.co/EurCvBsdL3          | []      
     2015-06-01 08:13:27.000 | 605346415993221121 | jacksondevs | Jackson, MS | The programming talent myth                                                                  +| []      
                             |                    |             |             |  https://t.co/UvQIh5FBhG                                                                      |         
     2015-06-01 08:13:33.000 | 605346443801600000 | nerdreich   |             | RT @neillyneil: "the most exciting part of computer programming" http://t.co/5aapjXmNZt       | []      
     2015-06-01 08:13:54.000 | 605346531894521857 | numb3r23    | London      | RT @stefstivala: 'the most exciting part of computer programming' <U+1F636><U+1F52B> http://t.co/7iWHkKq68k | []
     2015-06-01 08:15:05.000 | 605346829216022531 | hnbot       |             | The programming talent myth                                                                  +| []      
                             |                    |             |             | (Discussion on HN - http://t.co/1ojLRw77Vd) http://t.co/zMZmUSXoXJ                            |         
    (5 rows)
