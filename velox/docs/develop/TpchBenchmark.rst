=======================================
I/O Optimizations and the TpchBenchmark
=======================================

Introduction
============
This document is the outcome from a cycle of benchmarking to determine the best
I/O performance against AWS S3 Parquet data for TPCH. It is intended to describe
the tuning process recommendations and general suggestions for tuning the Velox
query engine.

Benchmarking in Velox is made easy with the optionally built TpchBenchmark
(velox_tpch_benchmark) executable. To build the benchmark executable
(*_build/release/velox/benchamrks/tpch/velox_tpch_benchmark*), use the
following command line to do the build with S3 support:

.. code:: shell

   $ make benchmarks-build EXTRA_CMAKE_FLAGS="-DVELOX_ENABLE_S3=ON"

----

Use Cases for Velox
===================

Velox is a library so there are multiple ways to use it. This document will
describe two models used by popular applications.

In-Process Multi-Threaded Executor Use Case
-------------------------------------------

This use case is used by `Presto <https://github.com/prestodb/presto>`_ and, as
it turns out, is the use case used by the *Velox TpchBenchmark Tool* below.
This use case uses a single multi-threaded process to perform execution of
queries in parallel. Each query is broken up into threads called **drivers**
via a planning algorithm.  Each **driver** may also have a thread pool to
perform I/O in a parallel manner. In the benchmarking tool both **driver**
thread count, and I/O thread count are exposed as command line configuration
options. In this use case, care must be taken to not create too many threads
since maximum number of threads is a product of **driver** threads and I/O
threads. In this use case, the application owns creating **drivers** and I/O
Thread pools for Velox. In the case of the benchmark tool, the tool is
responsible for both **driver** threads and I/O threads.

Multiple Process Executor Use Case
----------------------------------

This use case is used by Spark + `Gluten <https://github.com/oap-project/gluten>`_
and it differs from the Presto use case where parallelism is concerned. Spark
uses multiple processes where each process is a Gluten+Velox query processor.
Spark scales by using many Linux processes for query processing. In this case
this means that the **drivers** are outside of Velox and Gluten and is defined
by the Spark configuration and number of workers. Gluten takes on the role of
creating and exposing the I/O thread pool count to Spark as configuration and
then injecting the I/O thread pool into Velox for parallel I/O.

----

Built-In TpchBenchmark
======================

This tool uses the "in-process" multi-threaded executor use case. This tool
exposes quite a few benchmark options as command line options. It also
exposes many Velox internal options. This document will only cover a subset
of the options and possible best-known values for them.

The setup used in experiments leading to this document was an AWS instance
ri6-8xlarge (32 vCPUs; 256GB RAM). The values (or formulas) below are based on
these experiments.

TpchBenchmark Tool Optimizations in Velox
-----------------------------------------

The tool exposes the following options (Note: When passing options for the tool
use a single dash not a double dash; i.e. -option and not --option):

* *num_drivers* - As described above this represent the number of drivers or
  executors used to process the TPC-H queries.

* *num_io_threads* - This represents the number of I/O threads used per
  **driver** for I/O.

* *cache_gb* - How much memory is used for caching data locally in this
  process. Memory caching cannot be shared in the multiple process executor use
  case but works well in the single process, multi-threaded use case.

* *num_splits_per_file* - This is a row group optimization for the stored
  dataset for benchmarking.

**NOTE:** *There is a limitation on the implementation of the AWS SDK that
will cause failures (curl error 28) if the **driver** *threads times I/O threads
grow much beyond 350 threads. This only really effects the multi-threaded
**drivers** *use case like the benchmark tool. It is only known to be an issue
when running against AWS S3. However, the error is coming from the libcurl
library so it is possible other Cloud storage APIs could also be affected.*

Velox exposes other options used for tuning that are of interest:

* *max_coalesce_bytes* - Size of coalesced data, has small improvements as size
  grows.

* *max_coalesce_distance_bytes* - Maximum gap bytes between data that can
  coalesced. Larger may mean more fetched data but at greater bytes/sec.

Top Optimization Recommendations
--------------------------------

.. csv-table:: Starting Point for Tuning
   :header: "Option Name", "Single Process", "Multi-Process"
   :widths: auto

   "num_drivers","max(20, vCPUs<super>*</super> X 3 / 4)","NA"
   "num_io_threads", "max(16, vCPUs<super>*</super> X 3 / 8)", "vCPUs*"
   "cache_gb", "50% System RAM", "NA (default = 0)"
   "num_splits_per_file", "Row Group Size of Data", "Row Group Size of Data"
   "max_coalesced_bytes", "Minimum of 90MB", "Minimum of 90MB"
   "max_coalesced_distance_bytes", "Workload dependent**", "Workload dependent**"
   "parquet_prefetch_rowgroups", "Best is 1", "Best is 1"
   "split_preload_per_driver", "Best is 2", "Best is 2"
   "cache_prefetch_min_pct", "Best is 80", "Best is 80"

\*  vCPUs = (cores * hyper-threads)

\*\* Wide tables and few columns retrieved per row can lead to many I/O
requests, suggest increasing this value based on testing and the need to
reduce small I/O requests.

Optimizations for the TpchBenchmark (Single Process Use Case)
=============================================================

**num_drivers**
---------------

This configuration option describes the number of resources available to
process query plan tasks. This can greatly improve performance of CPU bound
workloads. The recommendation in the table above seems to be optimal for
TPCH-like workloads.

**num_io_threads**
------------------

This configuration option describes the number of threads available per driver
to retrieve data from the network source. If the workload is I/O bound, then
increasing this number is beneficial if the data is fewer requests of larger
chunks as opposed to many smaller requests.

**cache_gb**
------------

This configuration option is useful for workloads that read the same data
several times per query but only applies to the single process use case.
*NOTE: There is a SSD Caching option in Velox but it to is ONLY useful in
the single process use case.*

**num_splits_per_file**
-----------------------

This configuration option is best when the data set count of row groups
matches this value. The affect in overall performance appears based on
testing to be small, however.

Optimizations for All Workloads (Both Use Cases)
================================================

**max_coalesce_bytes**
----------------------

This configuration option is the maximum bytes coalesced into a single request
to the data source. This was tested from the default 128MB to 2GB, and the
overall improvement was small as size increased. Capturing request data did
show larger and fewer requests but not enough to vastly improve I/O performance.

**max_coalesce_distance_bytes**
-------------------------------

This configuration option is the maximum byte distance between needed data in
the same file at the data source that can be coalesced. Increasing this value
would theoretically reduce the number of requests and increase each request
size. However, if made too large the query will return too many un-needed bytes
and could decrease I/O performance. This plus __max_coalesce_bytes__ should be
fine-tuned for the workload being run.

Summary
=======

If a use of Velox matches the use case of the TcphBenchmark then it is a good
tool to test, I/O and driver performance for specific TCP-H queries. This would
benefit execution of specific production workloads that are like the chosen
queries. If in multi-process use case, like Spark/Gluten/Velox configuration,
the recommendation is to oversubscribe I/O threads between 2X and 3X vCPUs and
tune the 2 coalesce configurations exposed.

----

Appendix A: TpchBenchmark Tool Help Output
==========================================

From the repository root, use the following command line to see all the
available flags in the TpchBenchmark tool.

.. code:: shell

   $ ./_build/release/velox/benchmarks/tpch/velox_tpch_benchmark -helpon=TpchBenchmark
