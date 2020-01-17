=========================
Session Property Managers
=========================

Administrators can add session properties to control the behavior for subsets of their workload.
These properties are defaults and can be overridden by users (if authorized to do so). Session
properties can be used to control resource usage, enable or disable features, and change query
characteristics. Session property managers are pluggable.

Add an ``etc/session-property-config.properties`` file with the following contents to enable
the built-in manager that reads a JSON config file:

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
      }
    ]
