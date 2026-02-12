==========================
Presto C++ Runtime Metrics
==========================

.. contents::
    :local:
    :backlinks: none
    :depth: 2

Overview
========

Presto C++ workers expose various runtime metrics that can be collected and monitored when 
``runtime-metrics-collection-enabled`` is set to true. These metrics are available through the 
``GET /v1/info/metrics`` endpoint in Prometheus data format.

For information on enabling metrics collection, see :doc:`features`.

Executor Metrics
================

These metrics track the performance and queue sizes of various executors in the Presto C++ worker.

``presto_cpp.driver_cpu_executor_queue_size``
---------------------------------------------

* **Type:** gauge
* **Description:** Number of tasks currently queued in the driver CPU executor waiting to be processed.

``presto_cpp.driver_cpu_executor_latency_ms``
---------------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Description:** Latency distribution of tasks in the driver CPU executor, measuring the time from task submission to execution start.

``presto_cpp.spiller_executor_queue_size``
------------------------------------------

* **Type:** gauge
* **Description:** Number of spilling tasks currently queued in the spiller executor.

``presto_cpp.spiller_executor_latency_ms``
------------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Description:** Latency distribution of spilling tasks in the spiller executor.

``presto_cpp.http_executor_latency_ms``
---------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Description:** Latency distribution of HTTP request processing tasks in the HTTP executor.

HTTP Metrics
============

These metrics track HTTP requests and responses in the Presto C++ worker.

``presto_cpp.num_http_request``
-------------------------------

* **Type:** counter
* **Description:** Total number of HTTP requests received by the worker since startup.

``presto_cpp.num_http_request_error``
-------------------------------------

* **Type:** counter
* **Description:** Total number of HTTP request errors encountered by the worker since startup.

``presto_cpp.http_request_latency_ms``
--------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Description:** Latency distribution of HTTP request processing, from receipt to response.

``presto_cpp.http_request_size_bytes``
--------------------------------------

* **Type:** histogram
* **Unit:** bytes
* **Description:** Size distribution of HTTP request payloads.

HTTP Client Metrics
===================

These metrics track HTTP client connection behavior for outbound requests.

``presto_cpp.http.client.num_connections_created``
--------------------------------------------------

* **Type:** counter
* **Description:** Total number of HTTP client connections created by the worker.

``presto_cpp.http.client.connection_first_use``
-----------------------------------------------

* **Type:** counter
* **Description:** Number of HTTP requests that are the first request on a new connection (sequence number == 0).

``presto_cpp.http.client.connection_reuse``
-------------------------------------------

* **Type:** counter
* **Description:** Number of HTTP requests sent on reused connections (sequence number > 0).

``presto_cpp.http.client.transaction_create_delay_ms``
------------------------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Description:** Delay in creating HTTP client transactions.

Exchange Metrics
================

These metrics track data exchange operations between workers.

``presto_cpp.exchange_source_peak_queued_bytes``
------------------------------------------------

* **Type:** gauge
* **Unit:** bytes
* **Description:** Peak number of bytes queued in PrestoExchangeSource waiting to be consumed.

``presto_cpp.exchange.request.duration``
----------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Description:** Duration distribution of exchange data fetch requests.

``presto_cpp.exchange.request.num_tries``
-----------------------------------------

* **Type:** histogram
* **Description:** Number of retry attempts for exchange data fetch requests.

``presto_cpp.exchange.request.page_size``
-----------------------------------------
* **Type:** histogram
* **Unit:** bytes
* **Description:** Size distribution of data pages fetched through exchange requests.

``presto_cpp.exchange.get_data_size.duration``
----------------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Description:** Duration distribution of operations to get the size of exchange data.

``presto_cpp.exchange.get_data_size.num_tries``
-----------------------------------------------

* **Type:** histogram
* **Description:** Number of retry attempts for getting exchange data size.

Query Context and Memory Metrics
================================

These metrics track query execution contexts and memory usage.

``presto_cpp.num_query_contexts``
---------------------------------

* **Type:** gauge
* **Description:** Current number of active query contexts in the worker.

``presto_cpp.memory_manager_total_bytes``
-----------------------------------------

* **Type:** gauge
* **Unit:** bytes
* **Description:** Total bytes currently used by the memory manager across all queries' memory pools.

Task Metrics
============

These metrics track task lifecycle and execution states.

Task Counts
-----------

``presto_cpp.num_tasks``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of tasks created on this worker since startup.

``presto_cpp.num_tasks_bytes_processed``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Unit:** bytes
* **Description:** Total bytes processed by all tasks on this worker.

``presto_cpp.num_tasks_running``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Current number of tasks in running state.

``presto_cpp.num_tasks_finished``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of tasks that completed successfully.

``presto_cpp.num_tasks_cancelled``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of tasks that were cancelled.

``presto_cpp.num_tasks_aborted``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of tasks that were aborted.

``presto_cpp.num_tasks_failed``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of tasks that failed with an error.

``presto_cpp.num_tasks_planned``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of tasks that have been created but not yet started, including queued tasks.

``presto_cpp.num_tasks_queued``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of tasks currently waiting in the task queue.

Task Health Metrics
-------------------

``presto_cpp.num_zombie_velox_tasks``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of zombie Velox tasks (tasks that are no longer active but not cleaned up).

``presto_cpp.num_zombie_presto_tasks``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of zombie Presto tasks (tasks that are no longer active but not cleaned up).

``presto_cpp.num_tasks_with_stuck_operator``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of tasks that have at least one stuck operator.

``presto_cpp.num_cancelled_tasks_by_stuck_driver``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of tasks cancelled due to stuck drivers.

``presto_cpp.num_tasks_deadlock``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of tasks that encountered deadlock conditions.

``presto_cpp.num_tasks_manager_lock_timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Number of times the task manager lock acquisition timed out.

Driver Metrics
==============

These metrics track the state and execution of drivers within tasks.

Driver States
-------------

``presto_cpp.num_queued_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers currently queued and waiting to execute.

``presto_cpp.num_on_thread_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers currently executing on threads.

``presto_cpp.num_suspended_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers that are suspended.

Driver Blocking Reasons
-----------------------

``presto_cpp.num_blocked_wait_for_consumer_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers blocked waiting for downstream consumers to consume data.

``presto_cpp.num_blocked_wait_for_split_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers blocked waiting for new splits to be assigned.

``presto_cpp.num_blocked_wait_for_producer_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers blocked waiting for upstream producers to provide data.

``presto_cpp.num_blocked_wait_for_join_build_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers blocked waiting for join build side to complete.

``presto_cpp.num_blocked_wait_for_join_probe_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers blocked during join probe operations.

``presto_cpp.num_blocked_wait_for_merge_join_right_side_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers blocked waiting for merge join right side data.

``presto_cpp.num_blocked_wait_for_memory_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers blocked waiting for memory to become available.

``presto_cpp.num_blocked_wait_for_connector_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers blocked waiting for connector operations to complete.

``presto_cpp.num_blocked_yield_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers that have yielded execution.

``presto_cpp.num_stuck_drivers``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of drivers that appear to be stuck and not making progress.

Worker Overload Metrics
=======================

These metrics indicate when the worker is overloaded and may reject new work.

``presto_cpp.overloaded_mem``
-----------------------------

* **Type:** gauge
* **Description:** Exports 100 if the worker is overloaded in terms of memory usage, 0 otherwise.

``presto_cpp.overloaded_cpu``
-----------------------------

* **Type:** gauge
* **Description:** Exports 100 if the worker is overloaded in terms of CPU usage, 0 otherwise.

``presto_cpp.overloaded``
-------------------------

* **Type:** gauge
* **Description:** Exports 100 if the worker is overloaded in terms of either memory or CPU, 0 otherwise.

``presto_cpp.task_planned_time_ms``
-----------------------------------

* **Type:** gauge
* **Unit:** milliseconds
* **Description:** Average time tasks spend in the planned state (queued) before starting execution.

``presto_cpp.overloaded_duration_sec``
--------------------------------------

* **Type:** gauge
* **Unit:** seconds
* **Description:** Duration in seconds that the worker has been continuously overloaded, or 0 if not currently overloaded.

Output Buffer Metrics
=====================

These metrics track the partitioned output buffers used for shuffling data.

``presto_cpp.num_partitioned_output_buffer``
--------------------------------------------

* **Type:** gauge
* **Description:** Total number of output buffers currently managed by all OutputBufferManagers.

``presto_cpp.partitioned_output_buffer_get_data_latency_ms``
------------------------------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Description:** Latency distribution of getData() calls on OutputBufferManager instances.

Worker Runtime Metrics
======================

``presto_cpp.worker_runtime_uptime_secs``
-----------------------------------------

* **Type:** counter
* **Unit:** seconds
* **Description:** Worker runtime uptime in seconds after the worker process started. This metric tracks how long the worker has been running.

Operating System Metrics
========================

These metrics provide insight into OS-level resource usage by the worker process.

CPU Time Metrics
----------------

``presto_cpp.os_user_cpu_time_micros``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Unit:** microseconds
* **Description:** User CPU time consumed by the presto_server process since the process started.

``presto_cpp.os_system_cpu_time_micros``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Unit:** microseconds
* **Description:** System CPU time consumed by the presto_server process since the process started.

Page Fault Metrics
------------------

``presto_cpp.os_num_soft_page_faults``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of soft page faults (page faults that can be resolved without disk I/O) encountered by the presto_server process since startup.

``presto_cpp.os_num_hard_page_faults``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of hard page faults (page faults requiring disk I/O) encountered by the presto_server process since startup.

Context Switch Metrics
----------------------

``presto_cpp.os_num_voluntary_context_switches``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of voluntary context switches in the presto_server process (when the process yields the CPU voluntarily).

``presto_cpp.os_num_forced_context_switches``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Total number of involuntary context switches in the presto_server process (when the process is preempted by the OS).

Hive Connector Metrics
======================

These metrics track the performance of the Hive connector's file handle cache. The metrics include 
a placeholder ``{}`` in their name which is replaced with the connector name at runtime.

File Handle Cache Metrics
-------------------------

``presto_cpp.{connector}.hive_file_handle_cache_num_elements``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Description:** Number of elements currently in the Hive file handle cache.

``presto_cpp.{connector}.hive_file_handle_cache_pinned_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Unit:** bytes
* **Description:** Total size of pinned (in-use) entries in the Hive file handle cache.

``presto_cpp.{connector}.hive_file_handle_cache_cur_size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** gauge
* **Unit:** bytes
* **Description:** Current total size of the Hive file handle cache.

``presto_cpp.{connector}.hive_file_handle_cache_num_accumulative_hits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Cumulative number of cache hits in the Hive file handle cache since startup.

``presto_cpp.{connector}.hive_file_handle_cache_num_accumulative_lookups``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Cumulative number of cache lookups in the Hive file handle cache since startup.

``presto_cpp.{connector}.hive_file_handle_cache_num_hits``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Number of cache hits in the Hive file handle cache (recent window).

``presto_cpp.{connector}.hive_file_handle_cache_num_lookups``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** counter
* **Description:** Number of cache lookups in the Hive file handle cache (recent window).

Thread Pool Metrics
===================

These metrics track the state of various thread pools. The metrics include a placeholder ``{}`` 
in their name which is replaced with the thread pool name at runtime.

``presto_cpp.{pool}.num_threads``
---------------------------------

* **Type:** gauge
* **Description:** Current number of threads in the thread pool.

``presto_cpp.{pool}.num_active_threads``
----------------------------------------

* **Type:** gauge
* **Description:** Number of threads currently executing tasks in the thread pool.

``presto_cpp.{pool}.num_pending_tasks``
---------------------------------------

* **Type:** gauge
* **Description:** Number of tasks waiting to be executed in the thread pool.

``presto_cpp.{pool}.num_total_tasks``
-------------------------------------

* **Type:** counter
* **Description:** Total number of tasks that have been submitted to the thread pool since startup.

``presto_cpp.{pool}.max_idle_time_ns``
--------------------------------------

* **Type:** gauge
* **Unit:** nanoseconds
* **Description:** Maximum idle time for threads in the pool before they are terminated.

EventBase Violation Metrics
===========================

These metrics track violations of the EventBase (event loop) threading model.

``presto_cpp.exchange_io_evb_violation_count``
----------------------------------------------

* **Type:** counter
* **Description:** Number of times the exchange I/O EventBase threading model was violated (operations performed on wrong thread).

``presto_cpp.http_server_io_evb_violation_count``
-------------------------------------------------

* **Type:** counter
* **Description:** Number of times the HTTP server I/O EventBase threading model was violated.

Memory Pushback Metrics
=======================

These metrics track the memory pushback mechanism that helps prevent out-of-memory conditions.

``presto_cpp.memory_pushback_count``
------------------------------------

* **Type:** counter
* **Description:** Number of times the memory pushback mechanism has been triggered.

``presto_cpp.memory_pushback_latency_ms``
-----------------------------------------

* **Type:** histogram
* **Unit:** milliseconds
* **Range:** 0-100,000 ms (0-100 seconds)
* **Percentiles:** P50, P90, P99, P100
* **Description:** Latency distribution of memory pushback operations, measuring how long each pushback attempt takes.

``presto_cpp.memory_pushback_reduction_bytes``
----------------------------------------------

* **Type:** histogram
* **Unit:** bytes
* **Range:** 0-15 GB (150 buckets)
* **Percentiles:** P50, P90, P99, P100
* **Description:** Distribution of actual memory usage reduction achieved by each memory pushback attempt. This metric helps gauge the effectiveness of the memory pushback mechanism.

``presto_cpp.memory_pushback_expected_reduction_bytes``
-------------------------------------------------------

* **Type:** histogram
* **Unit:** bytes
* **Range:** 0-15 GB (150 buckets)
* **Percentiles:** P50, P90, P99, P100
* **Description:** Distribution of expected memory usage reduction for each memory pushback attempt. The expected reduction may differ from actual reduction as other threads might allocate memory during the pushback operation.

Additional Runtime Metrics
==========================

For additional runtime metrics related to specific subsystems:

* **S3 FileSystem Metrics:** When Presto C++ workers interact with S3, additional runtime metrics are collected. See the `Velox S3 FileSystem documentation <https://facebookincubator.github.io/velox/monitoring/metrics.html#s3-filesystem>`_.

* **Velox Metrics:** Metrics from the underlying Velox execution engine are also available. These are prefixed with ``velox.`` instead of ``presto_cpp.``. See the `Velox metrics documentation <https://facebookincubator.github.io/velox/monitoring/metrics.html>`_.

Accessing Metrics
=================

To access these metrics:

1. Enable metrics collection by setting ``runtime-metrics-collection-enabled=true`` in your worker configuration.

2. Query the metrics endpoint:

   .. code-block:: bash

      curl http://worker-host:7777/v1/info/metrics

3. The response will be in Prometheus text format, suitable for scraping by Prometheus or other monitoring systems.

Example Output
--------------

.. code-block:: text

   # TYPE presto_cpp_worker_runtime_uptime_secs counter
   presto_cpp_worker_runtime_uptime_secs{cluster="production",worker="worker-01"} 3600
   # TYPE presto_cpp_num_tasks_running gauge
   presto_cpp_num_tasks_running{cluster="production",worker="worker-01"} 42
   # TYPE presto_cpp_memory_manager_total_bytes gauge
   presto_cpp_memory_manager_total_bytes{cluster="production",worker="worker-01"} 8589934592

See Also
========

* :doc:`features` - For information on enabling metrics collection
* :doc:`properties` - For worker configuration properties
* `Velox Metrics Documentation <https://facebookincubator.github.io/velox/monitoring/metrics.html>`_ - For metrics from the Velox execution engine
