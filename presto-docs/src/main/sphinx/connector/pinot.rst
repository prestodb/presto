======================
Apache Pinot Connector
======================

The Apache Pinot connector allows querying and creating tables in an external Apache
Pinot database. This can be used to query pinot data or join pinot data with
something else.

Configuration
-------------

To configure the Pinot connector, create a catalog properties file
in ``etc/catalog`` named, for example, ``pinot.properties``, to
mount the Pinot connector as the ``pinot`` catalog.
Create the file with the following contents, replacing the
connection properties as appropriate for your setup:

.. code-block:: none

    connector.name=pinot
    pinot.controller-urls=controller_host1:9000,controller_host2:9000

Where the ``pinot.controller-urls`` property allows you to specify a
comma separated list of the pinot controller host/port pairs.

Multiple Pinot Servers
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Pinot clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Map Pinot Schema to Presto Schema
---------------------------------

In general Pinot schema to Presto schema mapping are pretty straight forward.
By default, the data type mapping follows the table below.

.. list-table::
   :widths: 100 100
   :header-rows: 1

   * - Pinot Data Type
     - Presto Data Type
   * - INT
     - INTEGER
   * - LONG
     - BIGINT
   * - FLOAT
     - DOUBLE
   * - DOUBLE
     - DOUBLE
   * - BYTES
     - VARBINARY
   * - STRING
     - VARCHAR

Since Pinot defines each field as dimension, metric or time(date_time) field,
it's possible to infer Presto data type ``DATE`` and ``TIMESTAMP``:

- A Pinot ``TIME`` field with timeGranularity ``{ "TimeFormat":"EPOCH", "TimeUnit":"DAYS", "TimeUnitSize": 1 }`` could be map to a ``DATE`` type.
- A Pinot ``TIME`` field with timeGranularity ``{ "TimeFormat":"EPOCH", "TimeUnit":"MILLISECONDS", "TimeUnitSize": 1 }`` could be map to a ``TIMESTAMP`` type.
- A Pinot ``DATE_TIME`` field with format ``1:DAYS:EPOCH`` could be map to a ``DATE`` type.
- A Pinot ``DATE_TIME`` field with format ``1:MILLISECONDS:EPOCH`` could be map to a ``TIMESTAMP`` type.

There are a few configurations that control this behavior:

* ``pinot.infer-date-type-in-schema``: This config is false by default.
  Setting it to true will infer a Pinot ``TIME``/``DATE_TIME`` field to ``DATE`` in Presto if possible.

* ``pinot.infer-timestamp-type-in-schema``: This config is false by default.
  Setting it to true will infer a Pinot ``TIME``/``DATE_TIME`` field to ``TIMESTAMP`` in Presto if possible.

Below is an example with config: ``pinot.infer-timestamp-type-in-schema=true``.

Sample Pinot Schema:

.. code-block:: JSON

  {
    "schemaName": "meetupRsvp",
    "dimensionFieldSpecs": [
      {
        "name": "venue_name",
        "dataType": "STRING"
      },
      {
        "name": "event_name",
        "dataType": "STRING"
      },
      {
        "name": "event_id",
        "dataType": "STRING"
      },
      {
        "name": "event_time",
        "dataType": "LONG"
      },
      {
        "name": "group_city",
        "dataType": "STRING"
      },
      {
        "name": "group_country",
        "dataType": "STRING"
      },
      {
        "name": "group_id",
        "dataType": "LONG"
      },
      {
        "name": "group_name",
        "dataType": "STRING"
      }
    ],
    "metricFieldSpecs": [
      {
        "name": "rsvp_count",
        "dataType": "INT"
      }
    ],
    "timeFieldSpec": {
      "incomingGranularitySpec": {
        "name": "mtime",
        "dataType": "LONG",
        "timeType": "MILLISECONDS"
      }
    }
  }

Sample Presto Schema:

.. code-block:: none

  table_catalog | table_schema | table_name |  column_name  | ordinal_position | column_default | is_nullable | data_type |  comment  | extra_info
  ---------------+--------------+------------+---------------+------------------+----------------+-------------+-----------+-----------+------------
  pinot         | default      | meetuprsvp | venue_name    |                1 | NULL           | YES         | varchar   | DIMENSION | NULL
  pinot         | default      | meetuprsvp | rsvp_count    |                2 | NULL           | YES         | integer   | METRIC    | NULL
  pinot         | default      | meetuprsvp | group_city    |                3 | NULL           | YES         | varchar   | DIMENSION | NULL
  pinot         | default      | meetuprsvp | event_id      |                4 | NULL           | YES         | varchar   | DIMENSION | NULL
  pinot         | default      | meetuprsvp | group_country |                5 | NULL           | YES         | varchar   | DIMENSION | NULL
  pinot         | default      | meetuprsvp | group_id      |                6 | NULL           | YES         | bigint    | DIMENSION | NULL
  pinot         | default      | meetuprsvp | group_name    |                7 | NULL           | YES         | varchar   | DIMENSION | NULL
  pinot         | default      | meetuprsvp | event_name    |                8 | NULL           | YES         | varchar   | DIMENSION | NULL
  pinot         | default      | meetuprsvp | mtime         |                9 | NULL           | YES         | timestamp | TIME      | NULL
  pinot         | default      | meetuprsvp | event_time    |               10 | NULL           | YES         | bigint    | DIMENSION | NULL

Querying Pinot
--------------

The Pinot catalog exposes all pinot tables inside a flat schema. The
schema name is immaterial when querying but running ``SHOW SCHEMAS``,
will show just one schema entry of ``default``.

The name of the pinot catalog is the catalog file you created above
without the ``.properties`` extension. 

For example, if you created a
file called ``mypinotcluster.properties``, you can see all the tables
in it using the command::

    SHOW TABLES from mypinotcluster.default

OR::

    SHOW TABLES from mypinotcluster.foo

Both of these commands will list all the tables in your pinot cluster.
This is because Pinot does not have a notion of schemas.

Consider you have a table called ``clicks`` in the ``mypinotcluster``.
You can see a list of the columns in the ``clicks`` table using either
of the following::

    DESCRIBE mypinotcluster.dontcare.clicks;
    SHOW COLUMNS FROM mypinotcluster.dontcare.clicks;

Finally, you can access the ``clicks`` table::

    SELECT count(*) FROM mypinotcluster.default.clicks;


How the Apache Pinot connector works
------------------------------------

The connector tries to push the maximal sub-query inferred from the
presto query into pinot. It can push down everything Pinot supports
including aggregations, group by, all UDFs etc. It generates the
correct Pinot query keeping Pinot's quirks in mind.

By default, it sends aggregation and limit queries to the Pinot broker
and does a parallel scan for non-aggregation/non-limit queries. The
pinot broker queries create a single split that lets the Pinot broker
do the scatter gather. Whereas, in the parallel scan mode, there is
one split created for one-or-more Pinot segments and the Pinot servers
are directly contacted by the Presto servers (ie., the Pinot broker is
not involved in the parallel scan mode)

There are a few configurations that control this behavior:
    
* ``pinot.prefer-broker-queries``: This config is true by default.
  Setting it to false will also create parallel plans for
  aggregation and limit queries.
* ``pinot.forbid-segment-queries``: This config is false by default.
  Setting it to true will forbid parallel querying and force all
  querying to happen via the broker.
* ``pinot.non-aggregate-limit-for-broker-queries``: To prevent
  overwhelming the broker, the connector only allows querying the
  pinot broker for ``short`` queries. We define a ``short`` query to
  be either an aggregation (or group-by) query or a query with a limit
  less than the value configured for
  ``pinot.non-aggregate-limit-for-broker-queries``. The default value
  for this limit is 25K rows.
