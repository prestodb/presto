===============
Resource Groups
===============

Resource groups place limits on resource usage, and can enforce queueing policies on
queries that run within them or divide their resources among sub-groups. A query
belongs to a single resource group, and consumes resources from that group (and its ancestors).
Except for the limit on queued queries, when a resource group runs out of a resource
it does not cause running queries to fail; instead new queries become queued.
A resource group may have sub-groups or may accept queries, but may not do both.

In PrestoDB, Resource Groups are a powerful tool for managing query execution and resource allocation.
They allow administrators to control how resources are allocated and utilized on a Presto cluster.

Resource Usage Limits
---------------------

Resource Groups can set limits on resource usage, such as CPU time, memory usage, or total
number of queries. This is particularly useful in multi-tenant environments where you want to ensure that no single user
or query monopolizes the system resources.

Resource Consumption
--------------------

A query belongs to a single resource group, and it consumes resources from that group,
as well as its parent groups. If a resource group runs out of a certain resource, it does not cause running
queries to fail. Instead, new queries will be queued until resources become available again.

Sub-Groups and Query Acceptance
-------------------------------

Sub-groups allow for hierarchical resource allocation, where each sub-group can have its own resource limits and
queueing policies.A resource group that accepts queries directly is a leaf group, and it executes queries using
its allocated resources.

The resource groups and associated selection rules are configured by a manager, which is pluggable.
Presto resource management can be done in two ways:

* File-Based Resource Management
* Database-Based Resource Management

File-Based Resource Management
------------------------------

In a file-based resource manager, configuration information about the resource groups is stored in a JSON file.
This file contains definitions of all resource groups and the rules to select them.
The configuration file is loaded and used at the start of the Presto server.
Any changes to the file require a restart of the Presto server to take effect.

Database-Based Resource Management
----------------------------------

In a database-based resource manager, configuration information about the resource groups is stored in a relational
database. The database contains definitions of all resource groups and the rules to select them.
Unlike the file-based resource manager, changes to the configuration in the database take effect
immediately and do not require a restart of the Presto server.

Both methods have their pros and cons. File-based resource management is simpler to set up but less flexible,
while database-based resource management is more complex to set up but offers more flexibility and dynamic changes.

Setting Up File-Based Resource Management
-----------------------------------------

You can use a File-based Resource Group Manager:

1. Add a file ``etc/resource-groups.properties``
2. Set the ``resource-groups.configuration-manager`` property to ``file``.
3. Create a JSON file with the definitions of your resource groups and rules.
4. Specify the location of this file in the Presto server's configuration file
   under the property ``resource-groups.config-file``.
5. To complete the setup, follow the steps in File Resource Group Manager.

Setting Up Database-Based Resource Management
---------------------------------------------

You can use a File-based or a Database-based Resource Group Manager:

1. Add a file ``etc/resource-groups.properties``
2. Set the ``resource-groups.configuration-manager`` property to ``db``.
3. Set up a relational database that Presto can access.
4. Create tables in the database for resource groups and selection rules.
5. Specify the database's JDBC URL in the Presto server's configuration file
   under the property ``resource-groups.config-db-url``.
6. To complete the setup, follow the steps in Database Resource Group Manager.


File Resource Group Manager
---------------------------

The File Resource Group Manager in PrestoDB is a way to manage resources using a JSON configuration file.
This file contains definitions of all resource groups and rules for selecting the appropriate resource group
for a given query.

Here's a brief guide on how to set it up:

1. Create a JSON file: This file should contain the definitions for your resource groups.
Each resource group can specify things like the maximum memory, the maximum queued queries,
and the maximum running queries.

2. Specify the file in the Presto configuration: In the etc directory of your Presto installation,
create a file named ``resource-groups.properties``. In this file, add a line that specifies the location of
your JSON file: ``resource-groups.config-file = <file_path>``, where ``<file_path>`` is the path to your JSON file.

3. Restart the Presto server: After creating the JSON file and pointing to it in the Presto configuration,
restart your Presto server. The new resource groups will take effect after the restart.

.. code-block:: text

    resource-groups.configuration-manager=file
    resource-groups.config-file=etc/resource-groups.json


Database Resource Group Manager
-------------------------------
The Database Resource Group Manager in PrestoDB is a way to manage resources using a relational database.

Here's a brief guide on how to set it up:

1. Set up a relational database: The database should be accessible by Presto.
It will be used to store the configurations of your resource groups.

2. Create tables for resource groups and selection rules: You need to create tables in the
database that will store the definitions of your resource groups and the rules for selecting
the appropriate resource group for a given query.

3. Specify the database in the Presto configuration: In the ``etc`` directory of your Presto installation,
create a file named ``resource-groups.properties``. In this file, add a line that specifies
the JDBC URL of your database: ``resource-groups.config-db-url = <jdbc_url>``, where ``<jdbc_url>`` is the
JDBC URL of your database.

Note : The database resource group manager loads the configuration from a relational database. The
supported databases are MySQL, PostgreSQL, and Oracle. With the Database Resource Group Manager, changes to the
configuration in the database take effect immediately and do not require a restart of the Presto server.
This allows for more flexibility and dynamic changes to the resource group configurations.

.. code-block:: text

    resource-groups.configuration-manager=db
    resource-groups.config-db-url=jdbc:mysql://localhost:3306/resource_groups
    resource-groups.config-db-user=username
    resource-groups.config-db-password=password

Here's an example of what the tables in your database might look like:

Resource Groups Table
---------------------

``resource_group_id`` : The ID of the resource group.
name: The name of the resource group.

``soft_memory_limit``: The maximum amount of memory that the resource group can use.
max_queued: The maximum number of queued queries that the resource group can have.

``hard_concurrency_limit``: The maximum number of queries that the resource group can run concurrently.

Selectors Table
---------------

``resource_group_id``: The ID of the resource group that this selector applies to.

``user_regex``: A regex pattern that matches the user names for this selector.

``source_regex``: A regex pattern that matches the source names for this selector.


Database Resource Group Manager Properties
------------------------------------------

================================================ =========================================================== ===============
Property name                                    Description                                                 Default
================================================ =========================================================== ===============
``resource-groups.config-db-url``                Database URL to load configuration from.                    ``none``

``resource-groups.config-db-user``               Database user to connect with.                              ``none``

``resource-groups.config-db-password``           Password for database user to connect with.                 ``none``

``resource-groups.max-refresh-interval``         Time period for which the cluster will continue to accept   ``1h``
                                                 queries after refresh failures cause configuration to
                                                 become stale.

``resource-groups.exact-match-selector-enabled`` Setting this flag enables usage of an additional            ``false``
                                                 ``exact_match_source_selectors`` table to configure
                                                 resource group selection rules defined exact name based
                                                 matches for source, environment and query type. By
                                                 default, the rules are only loaded from the ``selectors``
                                                 table, with a regex-based filter for ``source``, among
                                                 other filters.
================================================ =========================================================== ===============

Resource Group Properties
-------------------------
Resource Groups are defined using a set of properties that determine how resources are allocated and used.
Here are the key properties that can be set for a Resource Group

* ``name`` (required): The name of the resource group. This is a mandatory property.

* ``maxQueued`` (required): The maximum number of queries that can be queued in the resource group.
  If this limit is reached, new queries will be rejected.

* ``hardCpuLimit`` (optional): maximum amount of CPU time this
  group may use in a period.

* ``softMemoryLimit`` (required): The maximum amount of memory that the resource group can use.
  This can be specified in absolute terms (like "10GB") or as a percentage of the total available
  memory (like "50%").

* ``hardConcurrencyLimit`` (required): The maximum number of queries that the resource group
  can run concurrently.

* ``softConcurrencyLimit`` (optional): The soft limit on the number of concurrent queries.
  If this limit is exceeded, the scheduler will try to prevent new queries from starting,
  but it won't force running queries to stop.

* ``softCpuLimit`` (optional): maximum amount of CPU time this
  group may use in a period (see ``cpuQuotaPeriod``), before a penalty is applied to
  the maximum number of running queries. ``hardCpuLimit`` must also be specified.

* ``schedulingPolicy`` (optional): The policy that determines how queries are scheduled within the resource group.
  This can be set to ``fair``, ``weighted``, ``weighted_fair`` or ``query_priority``. May be one of four values:

   * ``fair`` (default): queued queries are processed first-in-first-out, and sub-groups
     must take turns starting new queries, if they have any queued.

   * ``weighted_fair``: sub-groups are selected based on their ``schedulingWeight`` and the number of
     queries they are already running concurrently. The expected share of running queries for a
     sub-group is computed based on the weights for all currently eligible sub-groups. The sub-group
     with the least concurrency relative to its share is selected to start the next query.

   * ``weighted``: queued queries are selected stochastically in proportion to their priority,
     specified via the ``query_priority`` :doc:`session property </sql/set-session>`. Sub groups are selected
     to start new queries in proportion to their ``schedulingWeight``.

   * ``query_priority``: all sub-groups must also be configured with ``query_priority``.
     Queued queries are selected strictly according to their priority.

* ``schedulingWeight`` (optional): The weight for the resource group when the parent group uses the ``weighted``
  scheduling policy. A higher weight means that the group gets a larger share of the parent group's
  resources.

* ``jmxExport`` (optional):  If set to ``true``, the statistics of the resource group will be exported via JMX.
  Defaults to ``false``.

* ``subGroups`` (optional): list of sub-groups.A list of sub-groups within the resource group.
  Each sub-group can have its own set of properties.

.. _scheduleweight-example:

Scheduling Weight Example
^^^^^^^^^^^^^^^^^^^^^^^^^

Schedule weighting is a method of assigning a priority to a resource. Sub-groups
with a higher scheduling weight are given higher priority. For example, to
ensure timely execution of scheduled pipelines queries, weight them higher than
adhoc queries.

Here's an example:

Let's say we have a root resource group ``global`` with two subgroups: ``engineering`` and ``marketing``.
The ``engineering`` subgroup has a scheduling weight of 3, and the ``marketing`` subgroup has a
scheduling weight of 1. In this setup, the engineering subgroup will get 75% of the
parent group's resources (because 3 is 75% of the total weight of 4), and the "marketing" subgroup will
get 25% of the parent group's resources (because 1 is 25% of the total weight of 4).

Schedule weighting allows you to prioritize certain subgroups over others in terms of resource allocation.
In this example, queries from the ``engineering`` subgroup will be prioritized over queries
from the ``marketing`` subgroup.

.. literalinclude:: schedule-weight-example.json
    :language: text

Selector Rules
--------------

Here are the key components of selector rules in PrestoDB:

* ``group`` (required): The group these queries will run in.

* ``user`` (optional): This is a regular expression that matches the user who is submitting the query.

* ``userGroup`` (optional): Regex to match against every user group the user belongs to.

* ``source`` (optional): This matches the source of the query, which is typically the
  application submitting the query.

* ``queryType`` (optional): String to match against the type of the query submitted:

    * ``SELECT``: ``SELECT`` queries.
    * ``EXPLAIN``: ``EXPLAIN`` queries (but not ``EXPLAIN ANALYZE``).
    * ``DESCRIBE``: ``DESCRIBE``, ``DESCRIBE INPUT``, ``DESCRIBE OUTPUT``, and ``SHOW`` queries.
    * ``INSERT``: ``INSERT``, ``CREATE TABLE AS``, and ``REFRESH MATERIALIZED VIEW`` queries.
    * ``UPDATE``: ``UPDATE`` queries.
    * ``DELETE``: ``DELETE`` queries.
    * ``ANALYZE``: ``ANALYZE`` queries.
    * ``DATA_DEFINITION``: Queries that alter/create/drop the metadata of schemas/tables/views,
      and that manage prepared statements, privileges, sessions, and transactions.

* ``clientTags`` (optional): List of tags. To match, every tag in this list must be in the list of
  client-provided tags associated with the query.


Selectors are processed sequentially and the first one that matches will be used.

Global Properties
-----------------

* ``cpuQuotaPeriod`` (optional): the period in which cpu quotas are enforced.
  The ``cpuQuotaPeriod`` is a global property often used in container-based environments to control the amount
  of CPU resources that a container can use in a specified period.

Please note that the exact implementation and naming of these properties can
vary between different container runtimes and orchestration systems.

Providing Selector Properties
-----------------------------

The source name can be set as follows:

* CLI: use the ``--source`` option.

* JDBC driver when used in client apps: add the ``source`` property to the
  connection configuration and set the value when using a Java application that
  uses the JDBC Driver.

* JDBC driver used with Java programs: add a property with the key ``source``
  and the value on the ``Connection`` instance as shown in :ref:`the example
  <jdbc-java-connection>`.

Client tags can be set as follows:

* CLI: use the ``--client-tags`` option.

* JDBC driver when used in client apps: add the ``clientTags`` property to the
  connection configuration and set the value when using a Java application that
  uses the JDBC Driver.

* JDBC driver used with Java programs: add a property with the key
  ``clientTags`` and the value on the ``Connection`` instance as shown in
  :ref:`the example <jdbc-parameter-reference>`.

Example
^^^^^^^

In the example configuration below, there are several resource groups, some of which are templates.
Templates allow administrators to construct resource group trees dynamically. For example, in
the ``pipeline_${USER}`` group, ``${USER}`` is expanded to the name of the user that submitted
the query. ``${SOURCE}`` is also supported, which is expanded to the source that submitted the
query. You may also use custom named variables in the ``source`` and ``user`` regular expressions.

There are four selectors, that define which queries run in which resource group:

* The first selector matches queries from ``bob`` and places them in the admin group.

* The second selector matches queries from ``admin`` user group and places them in the admin group.

* The third selector matches all data definition (DDL) queries from a source name that includes ``pipeline``
  and places them in the ``global.data_definition`` group. This could help reduce queue times for this
  class of queries, since they are expected to be fast.

* The fourth selector matches queries from a source name that includes ``pipeline``, and places them in a
  dynamically-created per-user pipeline group under the ``global.pipeline`` group.

* The fifth selector matches queries that come from BI tools which have a source matching the regular
  expression ``jdbc#(?<toolname>.*)`` and have client provided tags that are a superset of ``hipri``.
  These are placed in a dynamically-created sub-group under the ``global.adhoc`` group.
  The dynamic sub-groups are created based on the values of named variables ``toolname`` and ``user``.
  The values are derived from the source regular expression and the query user respectively.
  Consider a query with a source ``jdbc#powerfulbi``, user ``kayla``, and client tags ``hipri`` and ``fast``.
  This query is routed to the ``global.adhoc.bi-powerfulbi.kayla`` resource group.

* The last selector is a catch-all, which places all queries that have not yet been matched into a per-user
  adhoc group.

Together, these selectors implement the following policy:

* The user ``bob`` and any user belonging to user group ``admin``
  is an admin and can run up to 50 concurrent queries.
  Queries will be run based on user-provided priority.

For the remaining users:

* No more than 100 total queries may run concurrently.

* Up to 5 concurrent DDL queries with a source ``pipeline`` can run. Queries are run in FIFO order.

* Non-DDL queries will run under the ``global.pipeline`` group, with a total concurrency of 45, and a per-user
  concurrency of 5. Queries are run in FIFO order.

* For BI tools, each tool can run up to 10 concurrent queries, and each user can run up to 3. If the total demand
  exceeds the limit of 10, the user with the fewest running queries gets the next concurrency slot. This policy
  results in fairness when under contention.

* All remaining queries are placed into a per-user group under ``global.adhoc.other`` that behaves similarly.

File Resource Group Manager
---------------------------

.. literalinclude:: resources-groups-example.json
    :language: json

Database Resource Group Manager
-------------------------------

This example is for a MySQL database.

.. literalinclude:: resource-groups-example1.sql
    :language: sql

.. code-block:: sql

    -- global properties

    -- get ID of 'other' group
    SELECT resource_group_id FROM resource_groups WHERE name = 'other';  -- 4

    -- create '${USER}' group with 'other' as parent.
    INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, environment, parent) VALUES ('${USER}', '10%', 1, 100, 'test_environment', 4);

    -- create 'bi-${toolname}' group with 'adhoc' as parent
    INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, scheduling_policy, environment, parent) VALUES ('bi-${toolname}', '10%', 10, 100, 10, 'weighted_fair', 'test_environment', 3);

    -- create 'pipeline' group with 'global' as parent
    INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, jmx_export, environment, parent) VALUES ('pipeline', '80%', 45, 100, 1, true, 'test_environment', 1);

    -- create a root group 'global' with NULL parent
    INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_policy, jmx_export, environment) VALUES ('global', '80%', 100, 1000, 'weighted', true, 'test_environment');

    -- get ID of 'global' group
    SELECT resource_group_id FROM resource_groups WHERE name = 'global';  -- 1
    -- create two new groups with 'global' as parent
    INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, environment, parent) VALUES ('data_definition', '10%', 5, 100, 1, 'test_environment', 1);
    INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, environment, parent) VALUES ('adhoc', '10%', 50, 1, 10, 'test_environment', 1);

    -- get ID of 'adhoc' group
    SELECT resource_group_id FROM resource_groups WHERE name = 'adhoc';   -- 3

    -- create 'other' group with 'adhoc' as parent
    INSERT INTO resource_groups (name, soft_memory_limit, hard_concurrency_limit, max_queued, scheduling_weight, scheduling_policy, environment, parent) VALUES ('other', '10%', 2, 1, 10, 'weighted_fair', 'test_environment', 3);


    -- Selectors

    -- use ID of 'admin' resource group for selector
    INSERT INTO selectors (resource_group_id, user_regex, priority) VALUES ((SELECT resource_group_id FROM resource_groups WHERE name = 'admin'), 'bob', 6);

    -- use ID of 'admin' resource group for selector
    INSERT INTO selectors (resource_group_id, user_group_regex, priority) VALUES ((SELECT resource_group_id FROM resource_groups WHERE name = 'admin'), 'admin', 5);

    -- use ID of 'global.data_definition' resource group for selector
    INSERT INTO selectors (resource_group_id, source_regex, query_type, priority) VALUES ((SELECT resource_group_id FROM resource_groups WHERE name = 'data_definition'), '.*pipeline.*', 'DATA_DEFINITION', 4);

    -- use ID of 'global.pipeline.pipeline_${USER}' resource group for selector
    INSERT INTO selectors (resource_group_id, source_regex, priority) VALUES ((SELECT resource_group_id FROM resource_groups WHERE name = 'pipeline_${USER}'), '.*pipeline.*', 3);
