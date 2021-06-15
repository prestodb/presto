===============
Resource Groups
===============

Resource groups place limits on resource usage, and can enforce queueing policies on
queries that run within them or divide their resources among sub-groups. A query
belongs to a single resource group, and consumes resources from that group (and its ancestors).
Except for the limit on queued queries, when a resource group runs out of a resource
it does not cause running queries to fail; instead new queries become queued.
A resource group may have sub-groups or may accept queries, but may not do both.

The resource groups and associated selection rules are configured by a manager which is pluggable.
Add an ``etc/resource-groups.properties`` file with the following contents to enable
the built-in manager that reads a JSON config file:

.. code-block:: none

    resource-groups.configuration-manager=file
    resource-groups.config-file=etc/resource_groups.json

Change the value of ``resource-groups.config-file`` to point to a JSON config file,
which can be an absolute path, or a path relative to the Presto data directory.

Resource Group Properties
-------------------------

* ``name`` (required): name of the group. May be a template (see below).

* ``maxQueued`` (required): maximum number of queued queries. Once this limit is reached
  new queries will be rejected.

* ``hardConcurrencyLimit`` (required): maximum number of running queries.

* ``softMemoryLimit`` (required): maximum amount of distributed memory this
  group may use before new queries become queued. May be specified as
  an absolute value (i.e. ``1GB``) or as a percentage (i.e. ``10%``) of the cluster's memory.

* ``softCpuLimit`` (optional): maximum amount of CPU time this
  group may use in a period (see ``cpuQuotaPeriod``) before a penalty will be applied to
  the maximum number of running queries. ``hardCpuLimit`` must also be specified.

* ``hardCpuLimit`` (optional): maximum amount of CPU time this
  group may use in a period.

* ``schedulingPolicy`` (optional): specifies how queued queries are selected to run,
  and how sub-groups become eligible to start their queries. May be one of three values:

    * ``fair`` (default): queued queries are processed first-in-first-out, and sub-groups
      must take turns starting new queries (if they have any queued).

    * ``weighted_fair``: sub-groups are selected based on their ``schedulingWeight`` and the number of
      queries they are already running concurrently. The expected share of running queries for a
      sub-group is computed based on the weights for all currently eligible sub-groups. The sub-group
      with the least concurrency relative to its share is selected to start the next query.

    * ``weighted``: queued queries are selected stochastically in proportion to their priority
      (specified via the ``query_priority`` :doc:`session property </sql/set-session>`). Sub groups are selected
      to start new queries in proportion to their ``schedulingWeight``.

    * ``query_priority``: all sub-groups must also be configured with ``query_priority``.
      Queued queries will be selected strictly according to their priority.

* ``schedulingWeight`` (optional): weight of this sub-group. See above.
  Defaults to ``1``.

* ``jmxExport`` (optional): If true, group statistics are exported to JMX for monitoring.
  Defaults to ``false``.

* ``perQueryLimits`` (optional): specifies max resources each query in a
  resource group may consume before being killed. These limits are not inherited from parent groups.
  May set three types of limits:

    * ``executionTimeLimit`` (optional): Specify an absolute value (i.e. ``1h``)
       for the maximum time a query may take to execute.

    * ``totalMemoryLimit`` (optional): Specify an absolute value (i.e. ``1GB``)
       for the maximum distributed memory a query may consume.

    * ``cpuTimeLimit`` (optional): Specify Specify an absolute value (i.e. ``1h``)
       for the maximum CPU time a query may use.

* ``subGroups`` (optional): list of sub-groups.

Selector Rules
--------------

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

* ``group`` (required): the group these queries will run in.

Global Properties
-----------------

* ``cpuQuotaPeriod`` (optional): the period in which cpu quotas are enforced.

Selectors are processed sequentially and the first one that matches will be used.

Providing Selector Properties
-----------------------------

The source name can be set as follows:

  * CLI: use the ``--source`` option.

  * JDBC: set the ``ApplicationName`` client info property on the ``Connection`` instance.

Client tags can be set as follows:

  * CLI: use the ``--client-tags`` option.

  * JDBC: set the ``ClientTags`` client info property on the ``Connection`` instance.

Example
-------

In the example configuration below, there are several resource groups, some of which are templates.
Templates allow administrators to construct resource group trees dynamically. For example, in
the ``pipeline_${USER}`` group, ``${USER}`` will be expanded to the name of the user that submitted
the query. ``${SOURCE}`` is also supported, which will be expanded to the source that submitted the
query. You may also use custom named variables in the ``source`` and ``user`` regular expressions.

There are four selectors that define which queries run in which resource group:

  * The first selector matches queries from ``bob`` and places them in the admin group.

  * The second selector matches all data definition (DDL) queries from a source name that includes "pipeline"
    and places them in the ``global.data_definition`` group. This could help reduce queue times for this
    class of queries, since they are expected to be fast.

  * The third selector matches queries from a source name that includes "pipeline", and places them in a
    dynamically-created per-user pipeline group under the ``global.pipeline`` group.

  * The fourth selector matches queries that come from BI tools (which have a source matching the regular
    expression ``"jdbc#(?<tool_name>.*)"``), and have client provided tags that are a superset of "hi-pri".
    These are placed in a dynamically-created sub-group under the ``global.pipeline.tools`` group. The dynamic
    sub-group will be created based on the named variable ``tool_name``, which is extracted from the in the
    regular expression for source. Consider a query with a source "jdbc#powerfulbi", user "kayla", and
    client tags "hipri" and "fast". This query would be routed to the ``global.pipeline.bi-powerfulbi.kayla``
    resource group.

  * The last selector is a catch-all, which places all queries that have not yet been matched into a per-user
    adhoc group.

Together, these selectors implement the following policy:

* The user "bob" is an admin and can run up to 50 concurrent queries. Queries will be run based on user-provided
  priority.

For the remaining users:

* No more than 100 total queries may run concurrently.

* Up to 5 concurrent DDL queries with a source "pipeline" can run. Queries are run in FIFO order.

* Non-DDL queries will run under the ``global.pipeline`` group, with a total concurrency of 45, and a per-user
  concurrency of 5. Queries are run in FIFO order.

* For BI tools, each tool can run up to 10 concurrent queries, and each user can run up to 3. If the total demand
  exceeds the limit of 10, the user with the fewest running queries will get the next concurrency slot. This policy
  results in fairness when under contention.

* All remaining queries are placed into a per-user group under ``global.adhoc.other`` that behaves similarly.


.. code-block:: json

    {
      "rootGroups": [
        {
          "name": "global",
          "softMemoryLimit": "80%",
          "hardConcurrencyLimit": 100,
          "maxQueued": 1000,
          "schedulingPolicy": "weighted",
          "jmxExport": true,
          "subGroups": [
            {
              "name": "data_definition",
              "softMemoryLimit": "10%",
              "hardConcurrencyLimit": 5,
              "maxQueued": 100,
              "schedulingWeight": 1
            },
            {
              "name": "adhoc",
              "softMemoryLimit": "10%",
              "hardConcurrencyLimit": 50,
              "maxQueued": 1,
              "schedulingWeight": 10,
              "subGroups": [
                {
                  "name": "other",
                  "softMemoryLimit": "10%",
                  "hardConcurrencyLimit": 2,
                  "maxQueued": 1,
                  "schedulingWeight": 10,
                  "schedulingPolicy": "weighted_fair",
                  "subGroups": [
                    {
                      "name": "${USER}",
                      "softMemoryLimit": "10%",
                      "hardConcurrencyLimit": 1,
                      "maxQueued": 100
                    }
                  ]
                },
                {
                  "name": "bi-${tool_name}",
                  "softMemoryLimit": "10%",
                  "hardConcurrencyLimit": 10,
                  "maxQueued": 100,
                  "schedulingWeight": 10,
                  "schedulingPolicy": "weighted_fair",
                  "subGroups": [
                    {
                      "name": "${USER}",
                      "softMemoryLimit": "10%",
                      "hardConcurrencyLimit": 3,
                      "maxQueued": 10
                    }
                  ]
                }
              ]
            },
            {
              "name": "pipeline",
              "softMemoryLimit": "80%",
              "hardConcurrencyLimit": 45,
              "maxQueued": 100,
              "schedulingWeight": 1,
              "jmxExport": true,
              "subGroups": [
                {
                  "name": "pipeline_${USER}",
                  "softMemoryLimit": "50%",
                  "hardConcurrencyLimit": 5,
                  "maxQueued": 100
                }
              ]
            }
          ]
        },
        {
          "name": "admin",
          "softMemoryLimit": "100%",
          "hardConcurrencyLimit": 50,
          "maxQueued": 100,
          "schedulingPolicy": "query_priority",
          "jmxExport": true
        }
      ],
      "selectors": [
        {
          "user": "bob",
          "group": "admin"
        },
        {
          "source": ".*pipeline.*",
          "queryType": "DATA_DEFINITION",
          "group": "global.data_definition"
        },
        {
          "source": ".*pipeline.*",
          "group": "global.pipeline.pipeline_${USER}"
        },
        {
          "source": "jdbc#(?<tool_name>.*)",
          "clientTags": ["hipri"],
          "group": "global.adhoc.bi-${tool_name}.${USER}"
        },
        {
          "group": "global.adhoc.other.${USER}"
        }
      ],
      "cpuQuotaPeriod": "1h"
    }

