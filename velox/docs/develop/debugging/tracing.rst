=======
Tracing
=======

Background
----------

The query trace tool helps analyze and debug query performance and correctness issues. It helps prevent
interference from external noise in a production environment (such as storage, network, etc.) by allowing
replay of a part of the query plan and dataset in an isolated environment, such as a local machine.
This is much more efficient for query performance analysis and issue debugging, as it eliminates the need
to replay the whole query in a production environment.

How Tracing Tool Works
----------------------

The tracing process consists of two distinct phases: the tracing phase and the replaying phase. The
tracing phase is executed within a production environment, while the replaying phase is conducted in
a local development environment.

**Tracing Phase**

1. Trace replay required metadata, including the query plan fragment, query configuration,
   and connector properties, is recorded during the query task initiation.
2. Throughout query processing, each traced operator logs the input vectors or splits
   storing them in a designated storage location.
3. The metadata and splits are serialized in JSON format, and the operator data inputs are
   serialized using a `Presto serializer <https://prestodb.io/docs/current/develop/serialized-page.html>`_.

**Replaying Phase**

1. Read and deserialize the recorded query plan, extract the traced plan node, and assemble a plan
   fragment with customized source and sink nodes.
2. The source node reads the input from the serialized operator inputs on storage and the sink operator
   prints or logs out the execution stats.
3. Build a task with the assembled plan fragment in step 1. Apply the recorded query configuration and
   connector properties to replay the task with the same input and configuration setup as in production.

**NOTE**: The presto serialization might lose input vector encoding, such as lazy vector and nested dictionary
encoding, which affects the operator’s execution. Hence, it might not always be the same as in production.

.. image:: ../images/trace-arch.png
    :width: 600
    :align: center

Tracing Framework
-----------------

The tracing framework consists of three components:

1. **Query Trace Writers**: records the metadata, input data, and scan splits.
2. **Query Trace Readers**: load the metadata, read input data, and scan splits.
3. **Query Trace Replayers**: display query summaries or replay the execution of the target operator.

Trace Writers
^^^^^^^^^^^^^

There are three types of writers: `TaskTraceMetadataWriter`, `OperatorTraceInputWriter`,
and `OperatorTraceSplitWriter`. They are used in the prod or shadow environment to record
the real execution data.

**TaskTraceMetadataWriter** records the query metadata during task creation,
serializes, and writes them into a file in JSON format. There are two kinds
of metadata:

- Query configurations and connector properties are specified by the user per query.
  They can be serialized as JSON map objects (key-value pairs).
- Plan fragment of the task (also known as a plan node tree). It can be serialized
  as a JSON object, which is already supported in Velox.

**OperatorTraceInputWriter** records the input vectors from the target operator, it uses a Presto
serializer to serialize each vector batch and flush immediately to ensure that replay is possible
even if a crash occurs during execution. It is created during the target operator's initialization
and writes data in the `Operator::addInput` method during execution. It finishes when the target
operator is closed. However, it can finish early if the recorded data size exceeds the limit specified
by the user.

**OperatorTraceSplitWriter** captures the input splits from the target `TableScan` operator. It
serializes each split and immediately flushes it to ensure that replay is possible even if a crash
occurs during execution. Each split is serialized as follows:

.. code-block:: c++

  | length : uint32_t | split : JSON string | crc32 : uint32_t |


Query Trace Readers
^^^^^^^^^^^^^^^^^^^

Three types of readers correspond to the query trace writers: `TaskTraceMetadataReader`,
`OperatorTraceInputReader`, and `OperatorTraceSplitReader`. The replayers typically use
them in the local environment, which will be described in detail in the Query Trace Replayer section.

**TaskTraceMetadataReader** can load the query metadata JSON file and extract the query
configurations, connector properties, and a plan fragment. The replayer uses these to build
a replay task.

**OperatorTraceInputReader** reads and deserializes the input vectors in a tracing data file.
It is created and used by a `QueryTraceScan` operator which will be described in detail in
the **Query Trace Scan** section.

**OperatorTraceSplitReader** reads and deserializes the input splits in tracing split info files,
and produces a list of `exec::Split` for the query replay.

How To Replay
-------------

After the traced query finishes, its metadata and the input data for the target tasks and operators
are all saved in the directory specified by `query_trace_dir`.

To get a glance at the traced task, we can execute the following command:

.. code-block:: c++

  velox_query_replayer --root_dir /trace_root --task_id task-4 --summary

It would show something as the follows:

.. code-block:: c++

  ++++++Query trace summary++++++
  Number of tasks: 1

  ++++++Query configs++++++
  	query_trace_task_reg_exp: .*
  	query_trace_node_ids: 2
  	query_trace_max_bytes: 107374182400
  	query_trace_dir: /tmp/velox_test_aJqeFd/basic/traceRoot/
  	query_trace_enabled: 1

  ++++++Connector configs++++++

  ++++++Task query plan++++++
  -- HashJoin[2][INNER t0=u0] -> t0:BIGINT, t1:VARCHAR, t2:SMALLINT, t3:REAL, u0:BIGINT, u1:INTEGER, u2:SMALLINT, u3:VARCHAR
    -- TableScan[0][table: hive_table] -> t0:BIGINT, t1:VARCHAR, t2:SMALLINT, t3:REAL
    -- TableScan[1][table: hive_table] -> u0:BIGINT, u1:INTEGER, u2:SMALLINT, u3:VARCHAR

  ++++++Task Summaries++++++

  ++++++Task task-1++++++

  ++++++Pipeline 2++++++
  driver 0: opType HashProbe, inputRows 70720, peakMemory 108.00KB
  driver 1: opType HashProbe, inputRows 70720, peakMemory 108.00KB

  ++++++Pipeline 3++++++
  driver 0: opType HashBuild, inputRows 48000, peakMemory 4.51MB
  driver 1: opType HashBuild, inputRows 48000, peakMemory 2.25MB

Then we can re-execute the query using the following command in the terminal or use the same flags in your IDE to debug.

.. code-block:: c++

  velox_query_replayer --root_dir /Users/bytedance/work/native/trace --query_id query-1 --task_id task-1 --node_id 2

.. code-block:: c++

  Stats of replaying operator HashBuild : Output: 0 rows (0B, 0 batches), Cpu time: 48.63us, Wall time: 65.22us, Blocked wall time: 24.08ms, Peak memory: 4.51MB, Memory allocations: 16, Threads: 2, CPU breakdown: B/I/O/F (23.79us/0ns/14.46us/10.38us)

  Stats of replaying operator HashProbe : Output: 13578240 rows (1.17GB, 13280 batches), Cpu time: 3.99s, Wall time: 4.01s, Blocked wall time: 98.58ms, Peak memory: 108.00KB, Memory allocations: 12534, Threads: 2, CPU breakdown: B/I/O/F (8.52ms/1.59s/2.39s/20.29us)

  Memory usage: TaskCursorQuery_0 usage 0B reserved 0B peak 10.00MB
      task.test_cursor 1 usage 0B reserved 0B peak 10.00MB
          node.2 usage 0B reserved 0B peak 0B
              op.2.1.1.OperatorTraceScan usage 0B reserved 0B peak 0B
              op.2.1.0.OperatorTraceScan usage 0B reserved 0B peak 0B
          node.N/A usage 0B reserved 0B peak 0B
              op.N/A.0.1.CallbackSink usage 0B reserved 0B peak 0B
              op.N/A.0.0.CallbackSink usage 0B reserved 0B peak 0B
          node.1 usage 0B reserved 0B peak 10.00MB
              op.1.1.0.HashBuild usage 0B reserved 0B peak 4.51MB
              op.1.0.1.HashProbe usage 0B reserved 0B peak 108.00KB
              op.1.1.1.HashBuild usage 0B reserved 0B peak 2.25MB
              op.1.0.0.HashProbe usage 0B reserved 0B peak 108.00KB
          node.0 usage 0B reserved 0B peak 0B
              op.0.0.1.OperatorTraceScan usage 0B reserved 0B peak 0B
              op.0.0.0.OperatorTraceScan usage 0B reserved 0B peak 0B

Here is a full list of supported command line arguments.

* ``--root_dir``: The root directory where the replayer is reading the traced data, must be set.
* ``--summary``: Show the summary of the tracing including number of tasks and task ids.
  It also print the query metadata including query configs, connectors properties, and query plan in JSON format.
* ``--query_id``: Specify the target query ID, it must be set.
* ``--task_id``: Specify the target task ID, it must be set.
* ``--node_id``: Specify the target node ID, it must be set.
* ``--driver_ids``: Specify the target driver IDs to replay.
* ``--shuffle_serialization_format``: Specify the shuffle serialization format.
* ``--table_writer_output_dir``: Specify the output directory of TableWriter.
* ``--hiveConnectorExecutorHwMultiplier``: Hardware multiplier for hive connector.

Future Work
-----------

https://github.com/facebookincubator/velox/issues/9668
