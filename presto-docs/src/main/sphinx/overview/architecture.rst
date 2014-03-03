============
Architecture
============

This section outlines general guidelines of the architecture of Presto
and defines several architectural patterns you will notice in
Presto. 

------------------
Parallel Execution
------------------

Unlike Hive, Presto is designed to avoid unnecessary, intermediate
steps to execute a query.  Where a Hive query would create a pipeline
of Map Reduce operations punctuated by large amounts of I/O, a Presto
query is going to attempt to execute an entire query across a
distributed network of parallel nodes all at once.  In Presto,
everything is done in parallel.

Stages execute in parallel with one stage consuming data from another
as soon as it is available. Within stages, tasks are executed in
parallel. Tasks operate on splits with a parallel collection of
drivers each designed to process a single input and produce a single
output as fast as possible.

-------------------------
Optimization of Operators
-------------------------

All of Presto's operations on data are captured in operators which are
the lowest level functional unit in Presto's architecture. These
operations are translated to the most efficient Java Virtual Machine
instructions possible to achieve the lowest amount of overhead and
fastest execution times possible.

This low-level optimization allows Presto to extract as much
performance from individual worker nodes as possible without wasting
unnecessary cycles on object creation and heap management.

---------------------------------
Local Data Access (When Possible)
---------------------------------

This is a design decision made in Presto's support for Hive and HDFS
clusters, but it can apply to any data source storing large amounts of
data on a cluster.  When possible, Presto will put a source operator
on the same node that stores a particular piece of data to reduce the
amount of latency associated with network and disk access as much as
possible.

If a Presto query needs to scan the entirety of a 5 TB table in Hive
distributed across 20 storage nodes it is much faster to schedule
tasks to execute on the nodes that contain these splits. This is just
one of the design decisions which makes Presto a dramatic improvement
over Hive.

--------------------
Direct Memory Access
--------------------

This architecture approach and design decision differentiates Presto
from other systems developed in Java. Presto runs the JVM in what is
called "unsafe" mode. It accesses data directly in system memory. This
means that if Presto needs to scan several gigabytes of data stored on
a particuar node it doesn't need to wait for unnecessary memory
allocation, it can access data directly in memory.

This low-level optimization (along with various other low-level
optimizations) is another factor that contributes to Presto's ability
to blaze through vast amounts of data very quickly.

---------------------
Everything is RESTful
---------------------

All interactions between nodes in Presto are RESTful and the protocol
between all components is HTTP. There is no special "wire protocol"
used to increase performance in the system as RESTful interactions
provide the necessary performance to stream and deliver data from one
component to another.

-----------
Open Source
-----------

Presto is aggressively open source, and licensed under the Apache
Software License version 2.0.

----
Java
----

Presto is implemented in Java. Software developed in Java is easy to
distribute, package, and build. Mature tools like Maven allow Presto
to be distributed widely without requiring the Presto team to reinvent
many wheels, and Java is a platform that has shown a continued
commitment to increasing performance. 