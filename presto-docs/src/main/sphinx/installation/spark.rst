=========================
Executing Presto on Spark
=========================

When there is a need to execute long running queries, Presto offers
an option to scale the resource management using Spark, as it doesn'f offer
one currently by itself. This way Presto can be executed as a stand alone 
application on Spark.

Steps
-----

Download the Presto Spark package tarball, :maven_download:`spark-package` 
and the Presto Spark laucher, :maven_download:`spark-launcher`. Keep both the
files at, say, *example* directory. We assume here a two node Spark cluster
with 2 cores each, thus giving us total four cores.

The following is an example ``config.properties``:

.. code-block:: none

    query.hash-partition-count=10
    redistribute-writes=false
    task.concurrency=4
    task.max-worker-threads=4
    task.writer-count=4
    
 
The details about properties are available at :doc:`/admin/properties`.
Note that ``task.concurrency``, ``task.writer-count`` and 
``task.max-worker-threads`` are set to 4 each, since we have four cores
and want to synchronize with the relevant Spark submit arguments below. 
These values are to be adjusted to keep all executor cores busy and 
synchronize with Spark submit parameters.

To execute Presto on Spark, first start your Spark cluster, which we will 
assume have the URL *spark://spark-master:7077*. Keep your 
time consuming query in a file called, say, *query.sql*.

.. parsed-literal::

    /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-cores 4 \
    --conf spark.task.cpus=4 \
    --class com.facebook.presto.spark.launcher.PrestoSparkLauncher \
      presto-spark-launcher-\ |version|\ .jar \
    --package presto-spark-package-\ |version|\ .tar.gz \
    --config /presto/etc/config.properties \
    --catalogs /presto/etc/catalogs \
    --catalog hive \
    --schema default \
    --file query.sql

The details about configuring catalogs are at :ref:`catalog_properties`. In
Spark submit arguments, note the values of *executor-cores* (number of cores per
executor in Spark) and *spark.task.cpus* (number of cores to allocate to each task
in Spark). These are also equal to the number of cores (4 in this case) and are
same as some of the ``config.properties`` settings discussed above. This is to ensure that
single Presto on Spark task is run in a single Spark executor (This limitation may be
temporary and is introduced to avoid duplicating broadcasted hash table for every
task).

You can check the spark web ui to see Presto application status
