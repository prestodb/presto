============
Architecture
============

This section outlines general guidelines of the architecture of Presto
and defines several architectural patterns you will notice in Presto.

For an overview of the concepts in the Presto architecture, read the
concepts section.

-------------------------
Primary Goal: Performance
-------------------------

Presto's primary concern is performance, and the system has been
designed to make decisions that maximize performance at the expense of
memory consumption and other concerns. Presto nodes are designed to
extract as much performance from the nodes that are configured to run
on, and Presto at the largest scale requires a large amount of memory
to satisfy this commitment to performance.

------------------
Parallel Execution
------------------

Unlike Hive, Presto is designed to avoid unnecessary, intermediate
steps to execute a query.  Where a Hive query would create a pipeline
of Map Reduce operations punctuated by large amounts of I/O, a Presto
query is going to attempt to execute an entire query across a
distributed network of parallel nodes all at once.  In Presto,
everything is done in parallel.

Stages are not serialized, they execute in parallel with intermediate
stages consuming data from lower-level stages as soon as it is
available. Within stages, tasks are executed in parallel consuming as
many splits as possible all at once to tackle larger data sets even
faster. Tasks operate on splits with a parallel collection of drivers
each designed to process a single input and produce a single output as
fast as possible.

-------------------------
Optimization of Operators
-------------------------

Presto operates on data.  It aggregates data, it performs calculations
with data, and it transforms data. All of these operations are
captured in operators which are the lowest level functional unit in
Presto. Many of these functions has not just been optimized in Java,
it has been translated to Java Virtual Machine instructions to
implement both operators and chains of operators with the lowest
amount of overhead and the least amount of time possible.

This low-level optimization allows Presto to extract as much
performance from individual worker nodes as possible without wasting
unnecessary cycles on object creation and heap management to process
large amounts of data quickly. End-users and administrators are often
surprised by how quickly a particular stage can process terabytes of
data. The reason for this speed is this low-level optimization.

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
tasks to execute on the nodes that contain these splits. To do
otherwise is to add as huge amount of unnecessary data transfer and
disk access to a query. This is just one of the design decisions which
makes Presto a dramatic improvement over Hive.

--------------------
Direct Memory Access
--------------------

This architecture approach and design decision differentiates Presto
from other systems developed in Java. Most memory access in Java is
"managed". Java developers don't have access to a direct memory
address that is managed by the underlying operating system; instead,
memory is access via a virtual machine and this virtual machine
manages a heap of objects which acts as the virtual "memory" for a
Java process.  Each process maintains a distinct heap, and if you need
to access an object, Java has to allocate memory in the heap.

Java's approach to memory is what makes it possible for Java
developers to ignore the complications that accompany direct memory
access. Java developers don't have to worry about overflow or
accessing a memory address that falls outside of the process they are
executing in. This means that (unlike C developers) Java developers
don't have to worry about pointer arithmetic or calling functions like
alloc() to deal with memory allocation issues.

This ease, this freedom from memory issues, comes at a high cost of
performance. In Java, every time you read a piece of data you have to
allocate memory in the heap. Every time you need to access something
that is already in memory, there is a layer between your code and the
data it is trying to access. Presto runs the JVM in what is called
"unsafe" mode because it accesses data directly in system memory. This
means that if Presto needs to scan several gigabytes of data stored on
a particuar node it doesn't need to wait for needless memory
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

While most payloads are JSON, data delivered to both components and
clients is often encoded in the most efficient format for
processing. This committment to REST services was made to make is as
easy as possible to integrate other components with Presto.

-----------
Open Source
-----------

Presto is aggressively open source. The database isn't licensed under
an open source license that limits distribution in any way, and by
using the Apache Software License version 2.0 for all aspects of
Presto, the Presto development team and Facebook hope to establish
Presto as a center of a larger community of interested developers and
organizations.

----
Java
----

Presto is implemented in Java. Java is fast. Software developed in
Java is easy to distribute, package, and build. Mature tools like
Maven allow Presto to be distributed widely without requiring the
Presto team to reinvent many wheels, and Java is a platform that has
shown a continued commitment to increasing performance. Unlike
alternatives to Presto, people interested in building Presto from
source have to run a single build on a single platform, and with Java
the Presto team can focus on writing code to make Presto even faster
rather than on systems to deal with the quirks of different platforms.

Again, Java is fast. Presto is proof of that.