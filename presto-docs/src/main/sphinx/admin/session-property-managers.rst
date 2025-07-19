=========================
Session Property Managers
=========================

Administrators can add session properties to control the behavior for subsets of their workload.
These properties are defaults and can be overridden by users (if authorized to do so). Session
properties can be used to control resource usage, enable or disable features, and change query
characteristics. Session property managers are pluggable. A session property manager can either
be database- or file-based.

To enable a built-in manager that reads a JSON configuration file, add an
``etc/session-property-config.properties`` file with the following contents:

.. code-block:: none

    session-property-config.configuration-manager=file
    session-property-manager.config-file=etc/session-property-config.json

Change the value of ``session-property-manager.config-file`` to point to a JSON config file,
which can be an absolute path, or a path relative to the Presto data directory.

This configuration file consists of a list of match rules, each of which specify a list of
conditions that the query must meet, and a list of session properties that should be applied
by default. All matching rules contribute to constructing a list of session properties. Rules
are applied in the order they are specified. Rules specified later in the file override values
for properties that have been previously encountered.


For the database-based built-in manager, add an
``etc/session-property-config.properties`` file with the following contents:

.. code-block:: none

    session-property-manager.db.url=url
    session-property-manager.db.username=username
    session-property-manager.db.password=password
    session-property-manager.db.refresh-period=50s

This database consists of three tables: session specs, client tags and properties.

.. code-block:: text

    presto:tpch> DESCRIBE session_specs;
      Column                     |  Type   | Extra | Comment
    -----------------------------+---------+-------+---------
     spec_id                     | bigint  |       |
     user_regex                  | varchar |       |
     source_regex                | varchar |       |
     query_type                  | varchar |       |
     group_regex                 | varchar |       |
     client_info_regex           | varchar |       |
     override_session_properties | tinyint |       |
     priority                    | varchar |       |
    (8 rows)

.. code-block:: text

    presto:tpch> DESCRIBE session_client_tags;
      Column     |  Type   | Extra | Comment
    -------------+---------+-------+---------
     tag_spec_id | bigint  |       |
     client_tag  | varchar |       |
    (2 rows)

.. code-block:: text

    presto:tpch> DESCRIBE session_property_values;
      Column                |  Type   | Extra | Comment
    ------------------------+---------+-------+---------
     property_spec_id       | bigint  |       |
     session_property_name  | varchar |       |
     session_property_value | varchar |       |
    (3 rows)

Match Rules
-----------

* ``user`` (optional): regex to match against user name.

* ``source`` (optional): regex to match against source string.

* ``queryType`` (optional): string to match against the type of the query submitted:
    * ``DATA_DEFINITION``: Queries that alter/create/drop the metadata of schemas/tables/views, and that manage
      prepared statements, privileges, sessions, and transactions.
    * ``DELETE``: ``DELETE`` queries.
    * ``DESCRIBE``: ``DESCRIBE``, ``DESCRIBE INPUT``, ``DESCRIBE OUTPUT``, and ``SHOW`` queries.
    * ``EXPLAIN``: ``EXPLAIN`` queries.
    * ``INSERT``: ``INSERT`` and ``CREATE TABLE AS`` queries.
    * ``SELECT``: ``SELECT`` queries.

* ``clientTags`` (optional): list of tags. To match, every tag in this list must be in the list of
  client-provided tags associated with the query.

* ``group`` (optional): regex to match against the fully qualified name of the resource group the query is
  routed to.

* ``clientInfo`` (optional): regex to match against the client info text supplied by the client

* ``overrideSessionProperties`` (optional): boolean to indicate whether session properties should override client specified session properties.
  Note that once a session property has been overridden by ANY rule it remains overridden even if later higher precedence rules change the
  value, but don't specify override.

* ``sessionProperties``: map with string keys and values. Each entry is a system or catalog property name and
  corresponding value. Values must be specified as strings, no matter the actual data type.

Example
-------

Consider the following set of requirements:

* All queries running under the ``global`` resource group must have an execution time limit of 8 hours.

* All interactive queries are routed to subgroups under the ``global.interactive`` group, and have an execution time
  limit of 1 hour (tighter than the constraint on ``global``).

* All ETL queries (tagged with 'etl') are routed to subgroups under the ``global.pipeline`` group, and must be
  configured with certain properties to control writer behavior.

* All high memory ETL queries (tagged with 'high_mem_etl') are routed to subgroups under the ``global.pipeline`` group,
  and must be configured to enable :doc:`/admin/exchange-materialization`.

These requirements can be expressed with the following rules:

.. code-block:: json

    [
      {
        "group": "global.*",
        "sessionProperties": {
          "query_max_execution_time": "8h",
        }
      },
      {
        "group": "global.interactive.*",
        "sessionProperties": {
          "query_max_execution_time": "1h"
        }
      },
      {
        "group": "global.pipeline.*",
        "clientTags": ["etl"],
        "sessionProperties": {
          "scale_writers": "true",
          "writer_min_size": "1GB"
        }
      },
      {
        "group": "global.pipeline.*",
        "clientTags": ["high_mem_etl"],
        "sessionProperties": {
          "exchange_materialization_strategy": "ALL",
          "partitioning_provider_catalog": "hive",
          "hash_partition_count": 4096
        }
      }
    ]
