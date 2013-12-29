=========
Use Cases
=========

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
processing (OLTP) database load.

--------------
What Presto Is
--------------

Presto is a tool designed to efficiently query vast amounts of data
using distributed queries. If you work with terabytes or petabytes of
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

----------------
Who uses Presto?
----------------

Presto is an open source project that operates under the auspices of
Facebook. It was invented at Facebook, and the project continues to
be developed by both Facebook internal developers and a number of
third-party developers in the community.
