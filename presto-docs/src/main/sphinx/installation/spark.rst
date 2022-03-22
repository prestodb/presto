=========================
Executing Presto on Spark
=========================

Presto on Spark makes it possible to leverage Spark as an execution framework 
for Presto queries. This is useful for queries that we want to run on thousands 
of nodes, requires 10s or 100s of terabytes of memory, and consume many CPU years.

Spark provides several values adds like resource isolation, fine grained resource 
management, and Spark's scalable materialized exchange mechanism.

Steps
-----

Download the Presto Spark package tarball, :maven_download:`spark-package` 
and the Presto Spark laucher, :maven_download:`spark-launcher`. Keep both the
files at, say, *example* directory. We assume here a two node Spark cluster
with four cores each, thus giving us eight total cores.

The following is an example ``config.properties``:

.. code-block:: none

    task.concurrency=4
    task.max-worker-threads=4
    task.writer-count=4
     
The details about properties are available at :doc:`/admin/properties`.
Note that ``task.concurrency``, ``task.writer-count`` and 
``task.max-worker-threads`` are set to 4 each, since we have four cores per executor
and want to synchronize with the relevant Spark submit arguments below. 
These values should be adjusted to keep all executor cores busy and 
synchronize with :command:`spark-submit` parameters.

To execute Presto on Spark, first start your Spark cluster, which we will 
assume have the URL *spark://spark-master:7077*. Keep your 
time consuming query in a file called, say, *query.sql*. Run :command:`spark-submit`
command from the *example* directory created earlier:

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

The details about configuring catalogs are at :ref:`catalog_properties`. In
Spark submit arguments, note the values of *executor-cores* (number of cores per
executor in Spark) and *spark.task.cpus* (number of cores to allocate to each task
in Spark). These are also equal to the number of cores (4 in this case) and are
same as some of the ``config.properties`` settings discussed above. This is to ensure that
a single Presto on Spark task is run in a single Spark executor (This limitation may be
temporary and is introduced to avoid duplicating broadcasted hash tables for every
task).
  
