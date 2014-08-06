================
Use Cases
================

"Presto is a faster Hive," is something you've likely heard, but that
statement tells you nothing about what Presto is used for. To
appreciate Presto you'll need to understand the use cases that drive
its development. How do people use Presto?  Why was it created? and
What are the future use cases that are anticipated?

This section answers these questions and puts Presto into perspective
so that prospective administrators and end users know what to expect from
Presto.

-----------------
What Presto Isn't
-----------------

Since Presto is being called a "database" by many members of the
community it makes sense to begin with a definition of what Presto is
not.

Don't mistake the fact that Presto understands SQL with it providing
the features of a standard database. Presto is not a general-purpose
relational database.  It isn't a replacement for MySQL or Postgresql,
or Oracle. Presto was not designed to handle Online transaction
processing (OLTP) database load. If you are building a system that
needs to store data, that needs to understand transactions, and that
needs a general-purpose database stay away from Presto. It isn't for
your use case.

If you are looking for a general-purpose database that understands
SQL, can support transactions, and which can scale to the same level
as Presto (Hundreds of Petabytes), then you should look at
technologies like TBD..

--------------
What Presto Is
--------------

Presto is a tool designed to efficiently query vast amounts of data
using distributed queries. If you work with Terabytes or Petabytes of
data you are likely using tools that interact with Hadoop and
HDFS. Presto was designed as an alternative to tools that query HDFS
using pipelines of Map/Reduce jobs, namely Apache Hive, but Presto
isn't limited to accessing HDFS. Presto can be and has been extended
to apply its approach to different kinds of data sources incuding
traditional relational databases and other data sources such as
Cassandra.

Presto was designed to handle data analysis, queries that aggregate
large amounts of data, and queries that generate reports. Presto was
designed to handle loads that would be classified as OLAP.

Presto achieves orders of magnitude improvement over systems such as
Hive because it has been optimized for performance. The high-level
architecture of Presto and Presto's approach to handling and
transporting data has been built around highly-localized data access
and minimizing the overhead associated with data transfer and
disk-based I/O. Instead of carting around Terabytes of data and
storing it on disk between each stage of a query, Presto aims to do as
much analysis and aggregation in memory.

At a low level, unit operations on data in Presto have been heavily
optimized to make efficient use of hardware and resources. From direct
memory access to operations that have been written in low-level Java
Virtual Machine instructions to other JVM-level optimizations, every
effort has been made in Presto to squeeze as much performance as
possible out of the current hardware and software resources.

This commitment to performance that spans both Presto's high-level
architecture and the low-level implementation of operators and tasks
is what makes Presto fast. At a basic level, Presto is a distributed
query tool that has been optimized for performance, and this is the
origin of the name. Presto was designed to be presto.

----------------
Who uses Presto?
----------------

Presto is an open source project that operates under the auspices of
Facebook. It was invented at Facebook, and the project continues to
be developed by both Facebook internal developers and a number of
third-party developers in the community who have shown an interest in
using Presto to analyze data at scale.

The primary end-users of Presto are data analysts running queries to
analyze data. At the time Presto was invented this consisted of
several hudrend data analysts at Facebook running queries again
several hundred petabytes of data. At the time this section was
written the Presto community was expanding to include several
integrators and companies starting to use and contribute to Presto.