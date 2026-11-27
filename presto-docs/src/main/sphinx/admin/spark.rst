===============
Presto on Spark
===============

Presto on Spark makes it possible to leverage Spark as an execution engine for Presto queries.
This is useful for queries that need to run on thousands of nodes,
require 10s or 100s of terabytes of memory, and consume many CPU years.

Spark adds several useful features like resource isolation, fine grained resource 
management, and a scalable materialized exchange mechanism.

Installation
------------

Download the Presto Spark package tarball, :maven_download:`spark-package` 
and the Presto Spark launcher, :maven_download:`spark-launcher`. Keep both files in the same directory.
The example assumes there is a two-node Spark cluster with four cores each, which gives a total of eight cores.

The following is an example ``config.properties``:

.. code-block:: properties

    task.concurrency=4
    task.max-worker-threads=4
    task.writer-count=4

The details about properties are available at :doc:`/admin/properties`.
Note that ``task.concurrency``, ``task.writer-count`` and ``task.max-worker-threads`` are set to 4 each,
since there are four cores per executor and it aligned with Spark submit arguments below.
These values should be adjusted to keep all executor cores busy and
synchronize with :command:`spark-submit` parameters.

Execution
---------

To execute Presto on Spark, first start the Spark cluster, which is assumed to have
the URL *spark://spark-master:7077*. Save the query in a file, for example, with the named *query.sql*.
Run :command:`spark-submit` command from the directory where Presto on Spark is installed:

.. parsed-literal:: 

     /spark/bin/spark-submit \\
     --master spark://spark-master:7077 \\
     --executor-cores 4 \\
     --conf spark.task.cpus=4 \\ 
     --class com.facebook.presto.spark.launcher.PrestoSparkLauncher \\ 
       presto-spark-launcher-\ |version|\ .jar \\
     --package presto-spark-package-\ |version|\ .tar.gz \\ 
     --config /presto/etc/config.properties \\ 
     --catalogs /presto/etc/catalogs \\ 
     --catalog hive \\
     --schema default \\ 
     --file query.sql 

The details about configuring catalogs are at :ref:`installation/deployment:Catalog Properties`.
In Spark submit arguments, note the values of *executor-cores* (number of cores per
executor in Spark) and *spark.task.cpus* (number of cores to allocate to each task
in Spark). These are also equal to the number of cores (4 in the example) and are
same as some of the ``config.properties`` settings discussed above. This is to ensure that
a single Presto on Spark task is run in a single Spark executor (This limitation may be
temporary and is introduced to avoid duplicating broadcasted hash tables for every task).

Driver-side Metadata Sidecar
----------------------------

When the executors run on a native (Velox) execution engine, the driver can launch a
short-lived ``presto_server`` sidecar at bootstrap to register native-only functions
into the planner. See :ref:`Driver-side Metadata Sidecar Properties
<admin/properties:Driver-side Metadata Sidecar Properties>` for the configuration.

Narrowed Executor Classpath
---------------------------

When executors run on a native (Velox) execution engine, the executor JVM only coordinates
the native worker and never uses most of the jars in the package. Every jar the JVM opens
keeps a copy of the jar's ZIP central directory on the heap for the life of the process, so
a few large unused jars can cost tens of MiB of executor heap. A package can therefore carry
narrowed copies of ``lib`` and ``plugin`` that leave such jars out, and executors can be told
to load them.

The copies are directories of symbolic links next to the originals, created when the package
is built:

* ``lib-native`` holds one link for each file of ``lib`` that executors keep.
* ``plugin-native`` is used as the plugin directory, so it has the same layout as ``plugin``:
  an entry for each entry of ``plugin``, by the same name, either a link to the original or a
  directory of links to the files the plugin keeps. A plugin directory left without jars finds
  no plugin, so a plugin that would lose every jar must be linked whole.

Either copy may be left out or left empty, which keeps that directory whole. Make the links relative, so
that the package can be extracted anywhere, and distribute the package in an archive that
preserves symbolic links.

To make executors load the copies, set the following in ``config.properties``:

.. code-block:: none

    presto.spark.narrowed-executor-classpath-enabled=true

The property is ``false`` by default, and when it is not set every executor keeps the full
classpath. The copies are used only on cluster executors -- never on the driver, and never on
a local-mode executor -- and only when ``native-execution-enabled`` is ``true`` and a native
worker configuration is provided.

An executor uses a copy only if every entry in it is a symbolic link that resolves, or a
directory of such links, and only if ``plugin-native`` has the same entries as ``plugin``;
otherwise it uses the original directory and writes the reason to its standard error. The same happens if ``lib-native`` lacks the jar that provides the Presto on
Spark service, or a jar holding a class that service provider needs to load. If the Presto on
Spark service then fails to start on the narrowed classpath -- for example because a left-out
jar is needed only once the service or a plugin is being created -- the executor writes the
cause to its standard error and starts the service once more with the full classpath. Whatever
the failed attempt started stays in memory until the executor exits, so add the jar that caused
it back to the copy.

A query that runs on the Java engine (``native_execution_enabled=false``) is refused on an
executor whose classpath was narrowed, because the Java engine needs jars that native
execution does not. To run such queries, keep native execution enabled or remove the setting.
