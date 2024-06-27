============
Cache Fuzzer
============

Cache fuzzer is designed to test the correctness and the reliability of the
in-memory async data cache and the durable local SSD cache, and their
interactions such as staging from async data cache to SSD cache, and load the
cache miss data from SSD cache into async data cache.

During each iteration, the fuzzer performs the following actions steps by steps:
1. Creating a set of data files on local file system with varying sizes as source data files.
2. Setting up the async data cache with and without SSD using a specific configuration.
3. Performing parallel random reads from the source data files created in step1.

How to run
----------

Use velox_cache_fuzzer_test binary to run cache fuzzer:

::

    velox/exec/tests/velox_cache_fuzzer_test

By default, the fuzzer will go through 10 interations. Use --steps
or --duration-sec flag to run fuzzer for longer. Use --seed to
reproduce fuzzer failures.

Here is a full list of supported command line arguments.

* ``–-steps``: How many iterations to run. Each iteration generates and
  evaluates one tale writer plan. Default is 10.

* ``–-duration_sec``: For how long to run in seconds. If both ``-–steps``
  and ``-–duration_sec`` are specified, –duration_sec takes precedence.

* ``–-seed``: The seed to generate random expressions and input vectors with.

* ``–-num_threads``: Number of read threads.

* ``–-read_iteration_sec``: For how long each read thread should run (in seconds).

* ``–-num_source_files``: Number of data files to be created.

* ``–-min_source_file_bytes``: Minimum source file size in bytes.

* ``–-max_source_file_bytes``: Maximum source file size in bytes.

* ``–-memory_cache_bytes``: Memory cache size in bytes.

* ``–-ssd_cache_bytes``: Ssd cache size in bytes.

* ``–-num_ssd_cache_shards``: Number of SSD cache shards.

* ``–-ssd_checkpoint_interval_bytes``: Checkpoint after every
  ``--ssd_checkpoint_interval_bytes``/``--num_ssd_cache_shards`` written into
  each file. 0 means no checkpointing.

* ``–-enable_checksum``: Enable checksum write to SSD.

* ``–-enable_checksum_read_verification``: Enable checksum read verification
  from SSD.

If running from CLion IDE, add ``--logtostderr=1`` to see the full output.
