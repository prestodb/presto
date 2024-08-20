==================
Spark Query Runner
==================

Introduction
------------

The Spark Query Runner is a tool designed to facilitate the testing of Velox.
It helps ensure the correctness of Velox's computing against Spark and
provides a method for identifying potential issues in Velox's implementation.
Spark Query Runner is designed to run against Spark-3.5.1.

How It Works
------------

The Spark Query Runner operates by executing given SQL queries on Spark and
returning results as Velox data format, which allows the comparison of results
between Velox and Spark.

Since Spark 3.4, Spark Connect has introduced a decoupled client-server architecture
for Spark that allows remote connectivity to Spark clusters. From the client
perspective, Spark Connect mostly behaves as any other gRPC client, which is polyglot
and cross-platforms. During execution, the Spark Connect endpoint embedded on the
Spark Server receives and parses queries into Spark’s logical plan operators.
From there, the standard Spark execution process kicks in, ensuring that Spark
Connect leverages all of Spark’s optimizations and enhancements. Results are
streamed back to the client through gRPC as Arrow-encoded row batches.

In the Spark Query Runner, we use Spark Connect to submit queries to Spark and fetch
the results back to Velox. The steps for this process are as follows:

1. Provide the Spark SQL query to be executed. The query could be generated from Velox
   plan node or manually written.
2. Create a protobuf message `ExecutePlanRequest` from the SQL query. The protocols
   used by Spark Connect are defined in `Apache Spark <https://github.com/apache/spark/tree/v3.5.1/connector/connect/common/src/main/protobuf/spark/connect>`_.
3. Submit the message to SparkConnectService through gRPC API `ExecutePlan`.
4. Fetch Spark's results from execution response. Results are in Arrow IPC stream format,
   and can be read as Arrow RecordBatch by `arrow::ipc::RecordBatchReader`.
5. Convert Arrow RecordBatch as Velox vector for the comparison with Velox's results.

Usage
-----

To use the Spark Query Runner, you will need to deploy an executable Spark and start the
Spark Connect server with below command.

.. code-block::

    "$SPARK_HOME"/sbin/start-connect-server.sh --jars "$JAR_PATH"/spark-connect_2.12-3.5.1.jar


The jar of Spark Connect could be downloaded from `maven repository <https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.1/>`_.
If Spark Connect server is started successfully, you can see log as below. The server will
be started at `localhost:15002`.

.. code-block::

    INFO SparkConnectServer: Spark Connect server started at: 0:0:0:0:0:0:0:0%0:15002

Another option is to use Spark Query Runner in the docker image `ghcr.io/facebookincubator/velox-dev:spark-server`
provided by Velox. It includes an executable Spark and the start script. You can download
the image and run below command to start Spark connect server in it.

.. code-block::

    bash /opt/start-spark.sh

You can then provide the Spark Query Runner with the SQL query and the data to run the
query on. The tool will execute the query on Spark and return results as Velox data format.

Currently to use Spark as reference DB is only supported in aggregate fuzzer test, in which
the results from Velox and Spark are compared to check for any differences. If the results
match, it indicates that Velox is producing the correct output. If the results differ, it
suggests a potential issue in Velox that needs to be investigated. You can trigger its test
referring to :doc:`Fuzzer <fuzzer>`.
