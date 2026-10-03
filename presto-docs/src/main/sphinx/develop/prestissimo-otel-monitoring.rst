==================================================
Monitoring Prestissimo with OpenTelemetry
==================================================

When monitoring a Presto deployment using the native execution engine,
there is an interesting challenge: the coordinator and worker expose
their metrics in completely different ways.

The Presto coordinator runs as a Java process and exposes runtime
information through JMX. The Prestissimo worker, on the other hand,
runs the Velox C++ execution engine and already provides a
Prometheus-compatible metrics endpoint.

This means that collecting metrics from the entire Prestissimo cluster
requires two different approaches. In this article, we look at how to
collect metrics from both components using the OpenTelemetry Collector,
enrich those metrics with useful metadata, and export them to an
observability backend without coupling the architecture to a specific
vendor.

The setup described here was verified using Docker and Presto 0.298.1.

.. contents::
   :local:
   :depth: 2

Understanding the Architecture
-------------------------------

Before looking at configuration, it is worth understanding where the
metrics actually come from.

At a high level there are two metric pipelines.

**Coordinator pipeline**

The Presto coordinator is a Java application. Its runtime information
is exposed through JMX MBeans. Because the coordinator does not provide
a native Prometheus endpoint, we attach the
`JMX Prometheus Java Agent <https://github.com/prometheus/jmx_exporter>`_
to the coordinator JVM. The agent converts JMX MBeans into
Prometheus-format metrics and exposes them over HTTP on port ``9483``.

.. code-block:: text

   Presto Coordinator
          │
          ▼
         JMX
          │
          ▼
   JMX Prometheus Java Agent
          │
          ▼
   :9483/metrics
          │
          ▼
   OpenTelemetry Collector

**Native worker pipeline**

The Prestissimo worker runs the Velox C++ engine and already exposes a
Prometheus-compatible endpoint through its built-in HTTP server. No
additional exporter is needed.

.. code-block:: text

   Prestissimo Worker
         │
         ▼
      Velox C++
         │
         ▼
   Built-in Prometheus endpoint
         │
         ▼
   /v1/info/metrics
         │
         ▼
   OpenTelemetry Collector

The key point is that the additional JMX exporter is required only for
the Java coordinator. The native worker already exposes its metrics.

For a broader view of the available Prestissimo worker metrics, see
:doc:`/presto_cpp/metrics`. For coordinator JMX metrics, see
:doc:`/admin/jmx-metrics`.

Why OpenTelemetry Collector?
-----------------------------

Scraping both endpoints directly with Prometheus is possible, but using
the OpenTelemetry Collector provides a useful abstraction layer. The
Collector can:

* Scrape metrics from multiple sources
* Enrich metrics with resource attributes
* Apply memory limits and batching
* Export telemetry using different protocols and exporters

This keeps the collection and processing layer independent from the
final observability backend. When the backend changes, only the
exporter configuration needs to change.

.. code-block:: text

              Metric Sources
                    │
           ┌────────┴────────┐
           │                 │
        Presto             Velox
         JMX              Metrics
           │                 │
           └────────┬────────┘
                    ▼
           OpenTelemetry
              Collector
                    │
              Processing
                    │
           ┌────────┼────────┐
           ▼        ▼        ▼
          OTLP   Prometheus  Other
                 Remote Write Exporters

Prerequisites
--------------

The verified setup uses the following components:

.. list-table::
   :header-rows: 1
   :widths: 60 40

   * - Component
     - Version
   * - Docker
     - 29.2
   * - Docker Compose
     - v5.1
   * - Presto Java Coordinator
     - 0.298.1
   * - Prestissimo Native Worker
     - 0.298.1
   * - OpenTelemetry Collector Contrib
     - 0.104.0
   * - JMX Prometheus Java Agent
     - 0.20.0

The Contrib distribution of the OpenTelemetry Collector is used because
the setup also uses the ``httpcheck`` receiver. The worker should have
approximately 8 GB or more of available memory for this configuration.

Project Structure
------------------

A typical project layout looks like this:

.. code-block:: text

   prestissimo/
   ├── docker-compose.prestissimo.yaml
   ├── otel-collector-config.yaml
   │
   ├── jmx-exporter/
   │   ├── jmx_prometheus_javaagent.jar
   │   └── jmx-config.yaml
   │
   ├── coordinator/
   │   ├── config.properties
   │   ├── jvm.config
   │   ├── node.properties
   │   └── catalog/
   │       ├── tpch.properties
   │       ├── tpcds.properties
   │       └── tpchstandard.properties
   │
   └── worker/
       ├── config.properties
       ├── node.properties
       └── velox.properties

This separates the configuration for the coordinator, native worker,
JMX exporter, and OpenTelemetry Collector.

Collecting Coordinator Metrics
-------------------------------

Downloading the JMX Prometheus Java Agent
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create the directory and download version 0.20.0:

.. code-block:: bash

   mkdir -p prestissimo/jmx-exporter

   curl -L -o prestissimo/jmx-exporter/jmx_prometheus_javaagent.jar \
     "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar"

Configuring the JMX Exporter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The JMX exporter configuration in ``jmx-exporter/jmx-config.yaml``
determines how MBeans are converted into Prometheus metrics. For
example, Query Manager metrics can be mapped as follows:

.. code-block:: yaml

   rules:
     - pattern: 'com.facebook.presto.execution<name=QueryManager><>(\w+)\.TotalCount'
       name: presto_query_manager_$1_total
       type: COUNTER

     - pattern: 'com.facebook.presto.execution<name=QueryManager><>(\w+)\.OneMinute\.Rate'
       name: presto_query_manager_$1_rate1m
       type: GAUGE

     - pattern: 'com.facebook.presto.memory<name=ClusterMemoryManager><>ClusterMemoryBytes'
       name: presto_cluster_memory_bytes
       type: GAUGE

     - pattern: 'com.facebook.presto.memory<name=ClusterMemoryManager><>QueriesKilledDueToOutOfMemory'
       name: presto_oom_killed_queries_total
       type: COUNTER

     - pattern: 'java.lang<type=GarbageCollector, name=(\w+)><>CollectionCount'
       name: jvm_gc_collection_total
       type: COUNTER
       labels:
         gc: "$1"

     - pattern: 'java.lang<type=Memory><>(\w+)MemoryUsage.(\w+)'
       name: jvm_memory_$2_bytes
       type: GAUGE
       labels:
         area: "$1"

To expose all available MBeans without selecting individual ones, use:

.. code-block:: yaml

   rules: []

The tested environment exposes approximately 355 JMX MBeans.

Attaching the JMX Agent to the Coordinator JVM
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add the following line to ``coordinator/jvm.config``:

.. code-block:: text

   -javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9483:/opt/jmx-exporter/jmx-config.yaml

A complete ``jvm.config`` looks like:

.. code-block:: text

   -server
   -Xmx4G
   -XX:+UseG1GC
   -XX:G1HeapRegionSize=32M
   -XX:+ExplicitGCInvokesConcurrent
   -XX:+HeapDumpOnOutOfMemoryError
   -XX:+ExitOnOutOfMemoryError
   -XX:ReservedCodeCacheSize=512M
   -Djdk.attach.allowAttachSelf=true
   -Dfile.encoding=UTF-8
   -javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9483:/opt/jmx-exporter/jmx-config.yaml

Once Presto starts, the JMX exporter is available at:

.. code-block:: text

   http://presto-coordinator:9483/metrics

Port ``9483`` is primarily used inside the Docker network. Mapping it
to the host is useful for debugging and manual inspection.

Collecting Native Worker Metrics
---------------------------------

The Prestissimo worker runs the Velox C++ engine and already exposes
Prometheus metrics through its built-in HTTP server. No additional
exporter is needed.

The endpoint is:

.. code-block:: text

   http://presto-worker-native:8080/v1/info/metrics

.. note::

   The metrics path is ``/v1/info/metrics``, not ``/metrics``. This
   distinction is essential when configuring the OpenTelemetry Collector
   scrape target.

Configuring Worker Memory
^^^^^^^^^^^^^^^^^^^^^^^^^^

Native workers require an explicit memory limit. In
``worker/config.properties``, configure:

.. code-block:: properties

   discovery.uri=http://presto-coordinator:8080
   presto.version=0.298.1-9e1b45f
   http-server.http.port=8080
   shutdown-onset-sec=1
   runtime-metrics-collection-enabled=true
   system-memory-gb=8

The ``system-memory-gb`` property is important. Without an explicit
memory limit, Velox derives a value from the host environment. In the
tested setup, this resulted in an ``MmapAllocator`` startup failure.
Setting it explicitly prevents the worker from attempting to map more
memory than the environment can provide.

Keeping Coordinator and Worker Versions Aligned
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``presto.version`` value in ``worker/config.properties`` must match
the coordinator's version string exactly. The coordinator's
``/v1/info`` endpoint can be used to determine the correct value.

If the strings do not match, the worker may register incorrectly and
queries will fail with:

.. code-block:: text

   No nodes available to run query

Configuring the OpenTelemetry Collector
-----------------------------------------

With both metric sources available, the OpenTelemetry Collector can
scrape them using ``prometheus`` receivers.

Receivers
^^^^^^^^^^

**Coordinator receiver**

.. code-block:: yaml

   receivers:
     prometheus/coordinator_jmx:
       config:
         scrape_configs:
           - job_name: presto_coordinator_jmx
             scrape_interval: 15s
             static_configs:
               - targets:
                   - "presto-coordinator:9483"
                 labels:
                   service: presto-coordinator
                   cluster: prestissimo

**Native worker receiver**

The native worker requires the custom metrics path:

.. code-block:: yaml

     prometheus/worker_velox:
       config:
         scrape_configs:
           - job_name: presto_worker_velox
             scrape_interval: 15s
             metrics_path: /v1/info/metrics
             static_configs:
               - targets:
                   - "presto-worker-native:8080"
                 labels:
                   service: presto-worker-native
                   cluster: prestissimo
                   engine: velox

The ``metrics_path: /v1/info/metrics`` line is required. Without it,
the Collector scrapes the wrong endpoint and Velox metrics do not
appear.

Health Checks
^^^^^^^^^^^^^^

The ``httpcheck`` receiver monitors whether HTTP endpoints are
reachable, independently of the metric content they return:

.. code-block:: yaml

     httpcheck/presto:
       targets:
         - endpoint: "http://presto-coordinator:8080/v1/cluster"
         - endpoint: "http://presto-coordinator:8080/v1/info"
         - endpoint: "http://presto-worker-native:8080/v1/status"
       collection_interval: 15s

This produces an ``http_check_status`` metric for each target, covering
cluster availability, the coordinator information endpoint, and the
native worker status endpoint.

Processors
^^^^^^^^^^^

A memory limiter protects the Collector from unbounded memory growth:

.. code-block:: yaml

   processors:
     memory_limiter:
       check_interval: 5s
       limit_mib: 256
       spike_limit_mib: 64

Resource attributes identify the cluster and environment:

.. code-block:: yaml

     resource:
       attributes:
         - key: cluster
           value: prestissimo
           action: upsert
         - key: environment
           value: production
           action: upsert

Batching reduces export overhead:

.. code-block:: yaml

     batch:
       timeout: 10s
       send_batch_size: 1000

Exporters
^^^^^^^^^^

One of the main benefits of the OpenTelemetry Collector is that the
pipeline is not tied to a specific observability backend. The Collector
can export processed metrics using any supported exporter, such as OTLP,
Prometheus Remote Write, or others.

For local inspection, the Collector can expose a Prometheus-compatible
endpoint:

.. code-block:: yaml

   exporters:
     prometheus/local:
       endpoint: "0.0.0.0:8889"
       resource_to_telemetry_conversion:
         enabled: true

This is especially useful during development because it allows
inspection of the metrics after they have passed through the Collector.

Complete Pipeline
^^^^^^^^^^^^^^^^^^

The two Prometheus receivers feed a common metrics pipeline:

.. code-block:: yaml

   service:
     pipelines:

       metrics/presto:
         receivers:
           - prometheus/coordinator_jmx
           - prometheus/worker_velox
         processors:
           - memory_limiter
           - resource
           - batch
         exporters:
           - prometheus/local

       metrics/health:
         receivers:
           - httpcheck/presto
         processors:
           - memory_limiter
           - resource
           - batch
         exporters:
           - prometheus/local

Additional exporters can be added to any pipeline without changing the
receivers or processors.

Docker Networking
^^^^^^^^^^^^^^^^^^

The OpenTelemetry Collector must be able to reach the coordinator, the
native worker, and the telemetry backend. In a Docker Compose
environment this typically means the Collector joins both the
Prestissimo network and the observability network:

.. code-block:: yaml

   services:
     otel-collector:
       image: otel/opentelemetry-collector-contrib:0.104.0
       networks:
         - prestissimo-network
         - observability-network
       ports:
         - "0.0.0.0:8889:8889"
         - "0.0.0.0:13133:13133"
         - "0.0.0.0:55679:55679"
       volumes:
         - ./otel-collector-config.yaml:/etc/otelcol-contrib/config.yaml:ro
       command:
         - "--config=/etc/otelcol-contrib/config.yaml"

Starting the Stack
-------------------

Once the configuration is ready:

.. code-block:: bash

   cd prestissimo

   docker compose \
     -f docker-compose.prestissimo.yaml \
     up -d

Verifying the Setup
--------------------

Each layer can be verified independently.

**Coordinator JMX metrics**

.. code-block:: bash

   curl http://localhost:9483/metrics | head -20

Prometheus-formatted metrics confirm the JMX-to-Prometheus conversion
is working.

**Native worker Velox metrics**

.. code-block:: bash

   curl http://localhost:8080/v1/info/metrics \
     | grep "^# TYPE" \
     | head -10

Metric type declarations confirm the native worker's Prometheus
endpoint is working.

**OpenTelemetry Collector**

.. code-block:: bash

   # Check Collector health
   curl http://localhost:13133

   # Inspect processed metrics
   curl http://localhost:8889/metrics \
     | grep -E "^(velox_|presto_)" \
     | head -20

At this point, metrics have travelled through the complete pipeline:

.. code-block:: text

   Presto / Velox
         │
         ▼
   Prometheus endpoint
         │
         ▼
   OTel Collector
         │
         ▼
   Processed metrics
         │
         ▼
   :8889/metrics

Understanding the Metrics
--------------------------

Once the pipeline is working, the metrics fall into several categories.

Coordinator metrics
^^^^^^^^^^^^^^^^^^^^

These metrics reflect query activity, cluster memory, active nodes,
and JVM behavior:

.. code-block:: text

   presto_query_manager_*
   presto_cluster_memory_*
   presto_active_nodes
   jvm_memory_*
   jvm_gc_*
   jvm_threads_*

The documented setup identifies approximately 108 coordinator and JVM
metrics.

Velox memory metrics
^^^^^^^^^^^^^^^^^^^^^

Memory allocation is one of the most important areas to monitor in a
native execution engine:

.. code-block:: text

   velox_memory_allocator_allocated_bytes
   velox_memory_allocator_mapped_bytes
   velox_memory_allocator_total_used_bytes

Spill metrics
^^^^^^^^^^^^^^

When queries cannot keep all required data in memory, spilling
activity becomes visible through:

.. code-block:: text

   velox_spill_bytes
   velox_spill_input_bytes
   velox_spill_memory_bytes
   velox_spill_peak_memory_bytes
   velox_spill_rows_count
   velox_spill_files_count

Cache metrics
^^^^^^^^^^^^^^

Cache effectiveness can be measured with:

.. code-block:: text

   velox_memory_cache_num_hits
   velox_memory_cache_hit_bytes
   velox_memory_cache_num_evicts

   velox_ssd_cache_cached_bytes
   velox_ssd_cache_read_bytes
   velox_ssd_cache_written_bytes

Task and driver metrics
^^^^^^^^^^^^^^^^^^^^^^^^

Execution activity on the native worker is reflected in:

.. code-block:: text

   presto_cpp_num_tasks
   presto_cpp_num_tasks_running
   presto_cpp_num_driver_threads

   velox_task_splits_count
   velox_driver_yield_count

Memory arbitration metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Memory contention and arbitration activity can be observed through:

.. code-block:: text

   velox_arbitrator_requests_count
   velox_arbitrator_aborted_count
   velox_arbitrator_free_capacity_bytes
   velox_arbitrator_global_arbitration_count

CPU and operating-system metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

OS-level resource usage by the native worker process:

.. code-block:: text

   presto_cpp_os_user_cpu_time_micros
   presto_cpp_os_system_cpu_time_micros
   presto_cpp_os_num_voluntary_context_switches

   presto_cpp_overloaded
   presto_cpp_overloaded_cpu
   presto_cpp_overloaded_mem

These are useful when investigating whether a worker is becoming CPU-
or memory-constrained. For a complete list of native worker metrics,
see :doc:`/presto_cpp/metrics`.

Common Problems
----------------

Worker crashes with MmapAllocator SIGABRT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The native worker attempted to map more memory than is available.
Set an explicit memory limit in ``worker/config.properties``:

.. code-block:: properties

   system-memory-gb=8

Adjust the value to match the available memory in the environment.

No nodes available to run query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The worker's version string does not match the coordinator. Check
``presto.version`` in ``worker/config.properties`` and ensure it
matches the value returned by the coordinator's ``/v1/info`` endpoint.

No Velox metrics in OpenTelemetry Collector
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Prometheus receiver is using the wrong metrics path. The correct
configuration is:

.. code-block:: yaml

   metrics_path: /v1/info/metrics

Using ``/metrics`` will not return Velox metrics.

Native TPCH column naming errors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Java TPCH connector uses simplified column names while Velox
expects standard names. Add the following to the TPCH catalog
configuration:

.. code-block:: properties

   tpch.column-naming=STANDARD

TPC-DS configuration error
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ensure the TPC-DS catalog contains:

.. code-block:: properties

   tpcds.use-varchar-type=true

Useful Debugging Commands
--------------------------

Watch metric names flowing through the Collector:

.. code-block:: bash

   docker logs otel-collector-presto -f 2>&1 \
     | grep "Name:"

Watch metric exporter activity:

.. code-block:: bash

   docker logs otel-collector-presto -f 2>&1 \
     | grep "MetricsExporter"

Check a specific Velox metric:

.. code-block:: bash

   curl -s http://localhost:8889/metrics \
     | grep velox_spill_bytes

Check active nodes:

.. code-block:: bash

   curl -s http://localhost:8080/v1/node \
     | python3 -m json.tool

Inspect coordinator JMX MBeans:

.. code-block:: bash

   curl -s http://localhost:8080/v1/jmx/mbean \
     | python3 -c \
     "import sys,json; [print(b['objectName']) for b in json.load(sys.stdin)]"

Check Collector health:

.. code-block:: bash

   curl http://localhost:13133

Open OTel zPages in a browser:

.. code-block:: text

   http://localhost:55679/debug/tracez

See Also
---------

* :doc:`/presto_cpp/metrics` — Prestissimo native worker metrics reference
* :doc:`/admin/jmx-metrics` — Presto coordinator JMX metrics reference
* :doc:`/presto_cpp/features` — Enabling runtime metrics collection
* `JMX Prometheus Java Agent <https://github.com/prometheus/jmx_exporter>`_ — JMX to Prometheus bridge
* `OpenTelemetry Collector Contrib <https://github.com/open-telemetry/opentelemetry-collector-contrib>`_ — Collector distribution used in this setup
* `Velox Metrics Documentation <https://facebookincubator.github.io/velox/monitoring/metrics.html>`_ — Metrics from the underlying Velox execution engine
