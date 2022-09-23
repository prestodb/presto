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

Multiple Pinot Clusters
^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Pinot clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

Catalog Properties
^^^^^^^^^^^^^^^^^^

The following catalog configuration properties are available:

==========================================================  =============================================================================================================
Property Name                                               Description
==========================================================  =============================================================================================================
``pinot.controller-urls``                                   Pinot controller urls.
``pinot.controller-rest-service``                           Alternative rest endpoint for Pinot controller requests.
``pinot.rest-proxy-url``                                    Pinot rest proxy url.
``pinot.allow-multiple-aggregations``                       Push down a Pinot query with multiple aggregation functions, default is true.
``pinot.limit-large-for-segment``                           Cap the number of rows returned when pushing down non-aggregation segment query, default is 2147483647.
``pinot.topn-large``                                        Cap the TOP/LIMIT value when pushing down broker query, default is 10000.
``pinot.thread-pool-size``                                  Parameter to init Pinot server query client, default is 30.
``pinot.min-connections-per-server``                        Parameter to init Pinot server query client, default is 10.
``pinot.max-connections-per-server``                        Parameter to init Pinot server query client, default is 30.
``pinot.max-backlog-per-server``                            Parameter to init Pinot server query client, default is 30.
``pinot.idle-timeout``                                      Parameter to init Pinot server query client, default is 5 minutes.
``pinot.connection-timeout``                                Connection Timeout to talk to Pinot servers, default is 1 minute.
``pinot.metadata-expiry``                                   Pinot metadata cache expiration time, default is 2 minutes.
``pinot.estimated-size-in-bytes-for-non-numeric-column``    Estimated byte size for non-numeric column, default is 20.
``pinot.service-header-param``                              RPC service header key, default is "RPC-Service".
``pinot.caller-header-param``                               RPC service caller header key, default is "RPC-Caller".
``pinot.caller-header-value``                               RPC service caller header value, default is "presto".
``pinot.forbid-broker-queries``                             No broker request pushing down, default is false.
``pinot.forbid-segment-queries``                            No segment query pushing down, fail the query if broker query pushing down is not possible, default is false.
``pinot.rest-proxy-service-for-query``                      Use rest proxy endpoint for Pinot broker requests, default is false.
``pinot.use-date-trunc``                                    Use the new UDF dateTrunc in pinot that is more presto compatible, default is false.
``pinot.num-segments-per-split``                            Number of segments of the same host per split, default is 1.
``pinot.ignore-empty-responses``                            Ignore empty or missing pinot server responses, default is false.
``pinot.fetch-retry-count``                                 Retry count for retriable pinot data fetch calls, default is 2.
``pinot.non-aggregate-limit-for-broker-queries``            Max limit for non aggregate queries to the pinot broker, default is 25000.
``pinot.infer-date-type-in-schema``                         Infer Pinot DAYS epoch column to Presto DATE type, default is true.
``pinot.infer-timestamp-type-in-schema``                    Infer Pinot SECONDS epoch column to Presto TIMESTAMP type, default is true.
``pinot.mark-data-fetch-exceptions-as-retriable``           Retry Pinot request when failure, default is true.
``pinot.pushdown-topn-broker-queries``                      Allow pushing down query pattern to broker: aggregation + groupBy + orderBy, default is false.
``pinot.use-streaming-for-segment-queries``                 Use gRPC endpoint for pinot server queries, default is false.
``pinot.streaming-server-grpc-max-inbound-message-bytes``   Max inbound message bytes when init gRPC client, default is 128MB.
``pinot.proxy-enabled``                                     Pinot Cluster is behind a proxy, default is false.
``pinot.grpc-host``                                         Pinot gRPC host.
``pinot.grpc-port``                                         Pinot gRPC port.
``pinot.secure-connection``                                 Use https for all connections is false.
``pinot.override-distinct-count-function``                  Override 'distinctCount' function name, default is "distinctCount".
``pinot.extra-http-headers``                                Extra headers when sending HTTP based pinot requests to Pinot controller/broker. E.g. k1:v1,k2:v2.
``pinot.extra-grpc-metadata``                               Extra metadata when sending gRPC based pinot requests to Pinot broker/server/proxy. E.g. k1:v1,k2:v2.
``pinot.grpc-tls-key-store-path``                           TLS keystore file location for gRPC connection, default is empty (not needed)
``pinot.grpc-tls-key-store-type``                           TLS keystore type for gRPC connection, default is empty (not needed)
``pinot.grpc-tls-key-store-password``                       TLS keystore password, default is empty (not needed)
``pinot.grpc-tls-trust-store-path``                         TLS truststore file location for gRPC connection, default is empty (not needed)
``pinot.grpc-tls-trust-store-type``                         TLS truststore type for gRPC connection, default is empty (not needed)
``pinot.grpc-tls-trust-store-password``                     TLS truststore password, default is empty (not needed)
``pinot.controller-authentication-type``                    Pinot authentication method for controller requests. Allowed values are ``NONE`` and ``PASSWORD`` - defaults to ``NONE`` which is no authentication.
``pinot.controller-authentication-user``                    Controller username for basic authentication method.
``pinot.controller-authentication-password``                Controller password for basic authentication method.
``pinot.broker-authentication-type``                        Pinot authentication method for broker requests. Allowed values are ``NONE`` and ``PASSWORD`` - defaults to ``NONE`` which is no authentication.
``pinot.broker-authentication-user``                        Broker username for basic authentication method.
``pinot.broker-authentication-password``                    Broker password for basic authentication method.
==========================================================  =============================================================================================================

If ``pinot.controller-authentication-type`` is set to ``PASSWORD`` then both ``pinot.controller-authentication-user`` and
``pinot.controller-authentication-password`` are required.

If ``pinot.broker-authentication-type`` is set to ``PASSWORD`` then both ``pinot.broker-authentication-user`` and
``pinot.broker-authentication-password`` are required.

Session Properties
^^^^^^^^^^^^^^^^^^

The following session properties are available:

========================================================  ==================================================================
Property Name                                             Description
========================================================  ==================================================================
``pinot.forbid_broker_queries``                           Forbid queries to the broker.
``pinot.forbid_segment_queries``                          Forbid segment queries.
``pinot.ignore_empty_responses``                          Ignore empty or missing pinot server responses.
``pinot.connection_timeout``                              Connection Timeout to talk to Pinot servers.
``pinot.mark_data_fetch_exceptions_as_retriable``         Retry Pinot query on data fetch exceptions.
``pinot.retry_count``                                     Retry count for retriable pinot data fetch calls.
``pinot.use_date_trunc``                                  Use the new UDF dateTrunc in pinot that is more presto compatible.
``pinot.non_aggregate_limit_for_broker_queries``          Max limit for non aggregate queries to the pinot broker.
``pinot.pushdown_topn_broker_queries``                    Push down order by to pinot broker for top queries.
``pinot.num_segments_per_split``                          Number of segments of the same host per split.
``pinot.limit_larger_for_segment``                        Server query selection limit for large segment.
``pinot.override_distinct_count_function``                Override distinct count function to another function name.
``pinot.topn_large``                                      Cap the TOP/LIMIT value when pushing down broker query.
``pinot.controller_authentication_user``                  Controller username for basic authentication method.
``pinot.controller_authentication_password``              Controller password for basic authentication method.
``pinot.broker_authentication_user``                      Broker username for basic authentication method.
``pinot.broker_authentication_password``                  Broker password for basic authentication method.
========================================================  ==================================================================

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
