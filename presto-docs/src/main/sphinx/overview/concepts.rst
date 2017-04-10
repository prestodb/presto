===============
Presto Concepts
===============

To understand Presto you must first understand the terms and concepts
used throughout the Presto documentation.

While it's easy to understand statements and queries, as an end-user
you should have familiarity with concepts such as stages and shards to
take full advantage of Presto to execute efficient queries.  As a
Presto administrator or a Presto contributor you should understand how
Presto's concepts of stages map to tasks and how tasks contain a set
of drivers which process data.

This section provides a solid definition for the core concepts
referenced throughout Presto, and these sections are sorted from most
general to most specific.

--------------
Presto Servers
--------------

There are two types of Presto servers: coordinators and workers. The
following section explains the difference between the two.

^^^^^^^^^^^
Coordinator
^^^^^^^^^^^

The Presto coordinator is a server that is responsible for parsing
statements, planning queries, and managing Presto worker nodes.  It is
the "brains" of a Presto installation and is also the node to which a
client connects to submit statements for execution.

Every Presto installation must have at least one Presto coordinator
alongside zero or more Presto workers. The coordinator keeps track of
the activity on each worker and coordinates the delivery of data to
the components created to execute a query. Coordinators keep track of
a logical model of a query involving a series of stages which is then
translated into a series of connected tasks running on a cluster of
Presto workers.

Coordinators communicate with both workers and clients using a REST
API.

^^^^^^
Worker
^^^^^^

A Presto worker is a server in a Presto installation which is
responsible for executing tasks and processing data. Worker nodes
consume data and produce results for either other workers involved in
the same query or a coordinator acting as a go-between for a Presto
client.

When a Presto worker becomes available, it advertises itself using a
discovery server making itself available to a Presto coordinator for
task execution.

Workers communicate with both other workers and Presto coordinators
using a REST API.

---------------------------
Presto Data Source Concepts
---------------------------

Throughout this documentation you'll read terms such as connector,
catalog, schema, and table. These fundamental concepts cover Presto's
model of a particular data source and are described in the following
section.

^^^^^^^^^
Connector
^^^^^^^^^

A connector adapts Presto to a data source such as Hive or a
relational database. You can think of a connector the same way you
think of a driver for a database. It is an implementation of Presto's
SPI which allows Presto to interact with a resource using a standard
API.

Presto contains several built-in connectors including a connector for
JMX, a "system" connector which provides access to built-in system
tables, a Hive connector, and a connector designed to serve TPC-H benchmark
data. Many third-party developers have contributed connectors so that
Presto can access data in a variety of data sources.

Every catalog is associated with a specific connector. If you examine
a catalog configuration file, you will see that each contains a
mandatory property "connector.name" which is used by the Catalog
manager to create a connector for a given catalog. It is possible
to have more than one catalog use the same connector to access two
different instances of a similar database. For example, if you have
two Hive clusters, you can configure two catalogs which use the Hive
connector to query data from either database.

^^^^^^^
Catalog
^^^^^^^

A Presto catalog contains schemas and references a data source via a
connector.  For example, the JMX catalog is a built-in catalog in
Presto which provides access to JMX information via a JMX connector.
When you run a SQL statement in Presto, you are running it against a
catalog.  Other examples of catalogs include the Hive catalog to
connect to a Hive data source.

When addressing a table in Presto, the fully-qualified table name is
always rooted in a catalog. For example, a fully-qualified table name
of "hive.test_data.test" would refer to the test table in the
test_data schema in the hive catalog.

Catalogs are defined in properties files stored in the Presto
configuration directory.

^^^^^^
Schema
^^^^^^

A schema is grouping of tables. Think of a traditional relational
database such as Postgresql, MySQL, or Oracle. Each one of those
database products has the concept of a schema and a Presto schema maps
to the same concept. Tables are grouped into schemas to organize
tables into schemas which share a common purpose.

A Catalog and Schema together define a set of tables that can be
queried.  When accessing Hive or a relational database with Presto, a
schema refers to the same concept in the target database.  When
accessing a catalog such as JMX, schema simply refers to a set of
tables used to represent JMX information and does not directly
correspond to a similar concept in the underlying technology.

^^^^^
Table
^^^^^

Presto's concept of a table isn't too different from a table in a
relational database. A table contains rows which have data in a
series of named columns.

------------------
Presto Query Model
------------------

Presto executes SQL statements and turns these statements into queries
that are executed across a distributed network of coordinators and
workers.

^^^^^^^^^
Statement
^^^^^^^^^

Presto executes ANSI-compatible SQL statements.  When Presto
documentation refers to a statement we are refering to statements as
defined in the ANSI SQL standard which consists of clauses,
expressions, and predicates.

Some readers might be curious why this section lists seperate concepts
for statements and queries. This is necessary because, in Presto,
statements simply refer to the textual representation of a SQL
statement. When a statement is executed, Presto creates a query along
with a query plan that is then distributed across a series of Presto
workers.

^^^^^
Query
^^^^^

When Presto parses a statement it converts it into a query and creates
a distributed query plan which is then realized as a series of
interconnected stages running on Presto Workers. When you retrieve
information about a query in Presto, you receive a snapshot of every
component that is involved in producing a result set in response to a
statement.

The difference between a statement and a query is simple. A statement
can be thought of as the string that is passed to Presto while a query
refers to the configuration and components instantiated to execute
that statement. A query encompasses stages, tasks, splits, catalogs,
and other components and data sources working in concert to produce a
result.

^^^^^
Stage
^^^^^

When Presto executes a query it does so by breaking up the execution
into a hierarchy of stages. For example, if Presto needs to aggregate
data from one billion rows stored in Hive it does so by creating a
root stage to aggregate the output of several other stages all of
which are designed to implement different sections of a distributed
query plan.

The hierarchy of stages that comprises a query resembles a tree.
Every query has a "root" stage which is responsible for aggregating
the output from other stages. Stages are what the coordinator uses to
model a distributed query plan, but stages themselves don't run on
Presto workers.

Note: If you are a Presto end-user, everything beyond Stage in this
section isn't necessary to understand how Presto works from an
end-user perspective.

^^^^^^^^
Exchange
^^^^^^^^

Stages connect to one another using an exchange. An exchange is
responsible for receiving and transporting data from one stage to
another and for interacting with other stages to retrieve data.  A
stage that produces data has an exchange called an output buffer, and
a stage that consumes data has an exchange called an exchange client.

Note that data is retrieved from the lowest level stage directly
from a connector. This interaction between a stage and a connector
uses an operator called a source operator. For example, if a stage
retrieves data from HDFS, this isn't performed with an exchange
client, the retrieval happens from a source operator running in a
driver.

^^^^
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

^^^^^^
Driver
^^^^^^

Tasks contain one or more parallel drivers. Drivers act upon data and
combine operators to produce output that is then aggregated by a task
and then delivered to another task in a another stage. A driver is a
sequence of operator instances, or you can think of a driver as a
physical set of operators in memory. It is the lowest level of
parallelism in the Presto architecture. A driver has one input and
one output.

^^^^^^^^
Operator
^^^^^^^^

An Operator in Presto encapsulates the functionality of functions and
other operations which take data as input and generate data as output.
Operators execute within a driver as a driver is simply an
assembly of different operators which are then applied to individual
pieces of data within a split.

^^^^^
Split
^^^^^

Tasks operate on splits, and splits are sections of larger data
set. Stages at the lowest level of a distributed query plan retrieve splits
from connectors, and intermediate stages at a higher level of a
distributed query plan are designed to retrieve data from other
stages.

When Presto is scheduling a query, the coordinator will query a
connector for a list of all splits that are available for a table.
The coordinator keeps track of which machines are running which tasks
and what splits are being processed by which tasks.


.. NOTE: Chapter for Connectors

.. NOTE: Explain how to use the Cassandra connector
