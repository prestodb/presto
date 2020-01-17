===============
Presto Concepts
===============

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

To understand Presto you must first understand the terms and concepts
used throughout the Presto documentation.

While it's easy to understand statements and queries, as an end-user
you should have familiarity with concepts such as stages and splits to
take full advantage of Presto to execute efficient queries.  As a
Presto administrator or a Presto contributor you should understand how
Presto's concepts of stages map to tasks and how tasks contain a set
of drivers which process data.

This section provides a solid definition for the core concepts
referenced throughout Presto, and these sections are sorted from most
general to most specific.

Server Types
------------

There are two types of Presto servers: coordinators and workers. The
following section explains the difference between the two.

Coordinator
^^^^^^^^^^^

The Presto coordinator is the server that is responsible for parsing
statements, planning queries, and managing Presto worker nodes.  It is
the "brain" of a Presto installation and is also the node to which a
client connects to submit statements for execution. Every Presto
installation must have a Presto coordinator alongside one or more
Presto workers. For development or testing purposes, a single
instance of Presto can be configured to perform both roles.

The coordinator keeps track of the activity on each worker and
coordinates the execution of a query. The coordinator creates
a logical model of a query involving a series of stages which is then
translated into a series of connected tasks running on a cluster of
Presto workers.

Coordinators communicate with workers and clients using a REST API.

Worker
^^^^^^

A Presto worker is a server in a Presto installation which is responsible
for executing tasks and processing data. Worker nodes fetch data from
connectors and exchange intermediate data with each other. The coordinator
is responsible for fetching results from the workers and returning the
final results to the client.

When a Presto worker process starts up, it advertises itself to the discovery
server in the coordinator, which makes it available to the Presto coordinator
for task execution.

Workers communicate with other workers and Presto coordinators
using a REST API.

Data Sources
------------

Throughout this documentation, you'll read terms such as connector,
catalog, schema, and table. These fundamental concepts cover Presto's
model of a particular data source and are described in the following
section.

Connector
^^^^^^^^^

A connector adapts Presto to a data source such as Hive or a
relational database. You can think of a connector the same way you
think of a driver for a database. It is an implementation of Presto's
:doc:`SPI </develop/spi-overview>` which allows Presto to interact
with a resource using a standard API.

Presto contains several built-in connectors: a connector for
:doc:`JMX </connector/jmx>`, a :doc:`System </connector/system>`
connector which provides access to built-in system tables,
a :doc:`Hive </connector/hive>` connector, and a
:doc:`TPCH </connector/tpch>` connector designed to serve TPC-H benchmark
data. Many third-party developers have contributed connectors so that
Presto can access data in a variety of data sources.

Every catalog is associated with a specific connector. If you examine
a catalog configuration file, you will see that each contains a
mandatory property ``connector.name`` which is used by the catalog
manager to create a connector for a given catalog. It is possible
to have more than one catalog use the same connector to access two
different instances of a similar database. For example, if you have
two Hive clusters, you can configure two catalogs in a single Presto
cluster that both use the Hive connector, allowing you to query data
from both Hive clusters (even within the same SQL query).

Catalog
^^^^^^^

A Presto catalog contains schemas and references a data source via a
connector.  For example, you can configure a JMX catalog to provide
access to JMX information via the JMX connector. When you run a SQL
statement in Presto, you are running it against one or more catalogs.
Other examples of catalogs include the Hive catalog to connect to a
Hive data source.

When addressing a table in Presto, the fully-qualified table name is
always rooted in a catalog. For example, a fully-qualified table name
of ``hive.test_data.test`` would refer to the ``test`` table in the
``test_data`` schema in the ``hive`` catalog.

Catalogs are defined in properties files stored in the Presto
configuration directory.

Schema
^^^^^^

Schemas are a way to organize tables. Together, a catalog and schema
define a set of tables that can be queried. When accessing Hive or a
relational database such as MySQL with Presto, a schema translates to
the same concept in the target database. Other types of connectors may
choose to organize tables into schemas in a way that makes sense for
the underlying data source.

Table
^^^^^

A table is a set of unordered rows which are organized into named columns
with types. This is the same as in any relational database. The mapping
from source data to tables is defined by the connector.

Query Execution Model
---------------------

Presto executes SQL statements and turns these statements into queries
that are executed across a distributed cluster of coordinator and workers.

Statement
^^^^^^^^^

Presto executes ANSI-compatible SQL statements.  When the Presto
documentation refers to a statement, it is referring to statements as
defined in the ANSI SQL standard which consists of clauses,
expressions, and predicates.

Some readers might be curious why this section lists separate concepts
for statements and queries. This is necessary because, in Presto,
statements simply refer to the textual representation of a SQL
statement. When a statement is executed, Presto creates a query along
with a query plan that is then distributed across a series of Presto
workers.

Query
^^^^^

When Presto parses a statement, it converts it into a query and creates
a distributed query plan which is then realized as a series of
interconnected stages running on Presto workers. When you retrieve
information about a query in Presto, you receive a snapshot of every
component that is involved in producing a result set in response to a
statement.

The difference between a statement and a query is simple. A statement
can be thought of as the SQL text that is passed to Presto, while a query
refers to the configuration and components instantiated to execute
that statement. A query encompasses stages, tasks, splits, connectors,
and other components and data sources working in concert to produce a
result.

Stage
^^^^^

When Presto executes a query, it does so by breaking up the execution
into a hierarchy of stages. For example, if Presto needs to aggregate
data from one billion rows stored in Hive, it does so by creating a
root stage to aggregate the output of several other stages all of
which are designed to implement different sections of a distributed
query plan.

The hierarchy of stages that comprises a query resembles a tree.
Every query has a root stage which is responsible for aggregating
the output from other stages. Stages are what the coordinator uses to
model a distributed query plan, but stages themselves don't run on
Presto workers.

Task
^^^^

As mentioned in the previous section, stages model a particular
section of a distributed query plan, but stages themselves don't
execute on Presto workers. To understand how a stage is executed,
you'll need to understand that a stage is implemented as a series of
tasks distributed over a network of Presto workers.

Tasks are the "work horse" in the Presto architecture as a distributed
query plan is deconstructed into a series of stages which are then
translated to tasks which then act upon or process splits. A Presto
task has inputs and outputs, and just as a stage can be executed in
parallel by a series of tasks, a task is executing in parallel with a
series of drivers.

Split
^^^^^

Tasks operate on splits which are sections of a larger data
set. Stages at the lowest level of a distributed query plan retrieve
data via splits from connectors, and intermediate stages at a higher
level of a distributed query plan retrieve data from other stages.

When Presto is scheduling a query, the coordinator will query a
connector for a list of all splits that are available for a table.
The coordinator keeps track of which machines are running which tasks
and what splits are being processed by which tasks.

Driver
^^^^^^

Tasks contain one or more parallel drivers. Drivers act upon data and
combine operators to produce output that is then aggregated by a task
and then delivered to another task in another stage. A driver is a
sequence of operator instances, or you can think of a driver as a
physical set of operators in memory. It is the lowest level of
parallelism in the Presto architecture. A driver has one input and
one output.

Operator
^^^^^^^^

An operator consumes, transforms and produces data. For example, a table
scan fetches data from a connector and produces data that can be consumed
by other operators, and a filter operator consumes data and produces a
subset by applying a predicate over the input data.

Exchange
^^^^^^^^

Exchanges transfer data between Presto nodes for different stages of
a query. Tasks produce data into an output buffer and consume data
from other tasks using an exchange client.
