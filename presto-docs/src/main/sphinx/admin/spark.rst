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

Executor Classpath Manifest
---------------------------

When executors run on a native (Velox) execution engine, the executor JVM only coordinates
the native worker and never uses most of the jars in the package. Every jar the JVM opens
keeps a copy of the jar's ZIP central directory on the heap for the life of the process, so
a few large unused jars can cost tens of MiB of executor heap. An executor classpath manifest
lists jars that executors leave off their classpath.

To use one, set ``presto.spark.executor-classpath-manifest`` in ``config.properties`` to the
path of the manifest. A relative path is resolved against the Presto on Spark package
directory:

.. code-block:: none

    presto.spark.executor-classpath-manifest=native-executor-classpath.txt

The property is not set by default, and when it is not set every executor keeps the full
classpath. The manifest is applied only on cluster executors -- never on the driver, and
never on a local-mode executor -- and only when ``native-execution-enabled`` is ``true`` and
a native worker configuration is provided.

The manifest has two sections, each listing one jar file name per line. ``#`` starts a
comment:

.. code-block:: none

    # jars the executor's lib/ class loader leaves out
    [lib-exclude]
    spark-core-3.4.1-1.jar

    # jars left out of every plugin directory
    [plugin-exclude]
    hudi-presto-bundle-0.14.0.jar

Every jar that is not listed stays on the classpath, including jars added to the package
later, and a listed jar that is not in the package is ignored. ``[plugin-exclude]`` is
applied through :ref:`admin/properties:\`\`plugin.excluded-jars\`\``. A missing or empty
section excludes nothing.

If the manifest cannot be read or is malformed -- a line that is neither a section header nor
a ``.jar`` file name, an unknown section, or an entry before the first section -- the executor
keeps the full classpath and writes the reason to its standard error. The same happens if the
manifest excludes the jar that provides the Presto on Spark service.

A query that runs on the Java engine (``native_execution_enabled=false``) is refused on an
executor whose classpath was narrowed, because the Java engine needs jars that native
execution does not. To run such queries, keep native execution enabled or remove the setting.
