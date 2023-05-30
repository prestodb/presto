=========
Use Cases
=========

This section puts Presto into perspective so that prospective
administrators and end users know what to expect from Presto.

------------------
What Presto Is Not
------------------

Since Presto is being called a *database* by many members of the community,
it makes sense to begin with a definition of what Presto is not.

Do not mistake the fact that Presto understands SQL with it providing
the features of a standard database. Presto is not a general-purpose
relational database. It is not a replacement for databases like MySQL,
PostgreSQL or Oracle. Presto was not designed to handle Online
Transaction Processing (OLTP). This is also true for many other
databases designed and optimized for data warehousing or analytics.

--------------
What Presto Is
--------------

Presto is a tool designed to efficiently query vast amounts of data
using distributed queries. If you work with terabytes or petabytes of
data, you are likely using tools that interact with Hadoop and HDFS.
Presto was designed as an alternative to tools that query HDFS
using pipelines of MapReduce jobs such as Hive or Pig, but Presto
is not limited to accessing HDFS. Presto can be and has been extended
to operate over different kinds of data sources including traditional
relational databases and other data sources such as Cassandra.

Presto was designed to handle data warehousing and analytics: data analysis,
aggregating large amounts of data and producing reports. These workloads
are often classified as Online Analytical Processing (OLAP).

----------------
Who uses Presto?
----------------

Presto is an open source project that operates under the governance of the
Presto Foundation, which is part of the
`Linux Foundation <https://www.linuxfoundation.org>`_.
Presto was invented at Meta, and continues to be developed by internal
developers at Meta and by third-party developers in the community.
