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

Use velox_cache_fuzzer binary to run cache fuzzer:

::

    velox/exec/fuzzer/velox_cache_fuzzer

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

* ``–-source_file_bytes``: Source file size in bytes. When set to -1, a random
  value from 32MB to 48MB will be used, inclusively.

* ``–-memory_cache_bytes``: Memory cache size in bytes. When set to -1, a
  random value from 48 to 64MB will be used, inclusively.

* ``–-ssd_cache_bytes``: SSD cache size in bytes. When set to -1, 1 out of
  10 times SSD cache will be disabled, while the other times, a random value
  from 128MB to 256MB will be used, inclusively.

* ``–-num_ssd_cache_shards``: Number of SSD cache shards. When set to -1, a
  random value from 1 to 4 will be used, inclusively.

* ``–-ssd_checkpoint_interval_bytes``: Checkpoint after every
  ``--ssd_checkpoint_interval_bytes``/``--num_ssd_cache_shards``, written into
  each file. 0 means no checkpointing. When set to -1, 1 out of 4 times
  checkpoint will be disabled, while the other times, a random value from 32MB
  to 64MB will be used, inclusively. Checkpoint after every written into each
  file. 0 means no checkpointing.

* ``–-num_restarts``: Number of cache restarts in one iteration.

* ``–-enable_file_faulty_injection``: Enable fault injection on read and write
  operations for cache-related files. When enabled, the file read and write
  operations will fail 5 out of 100 times.

If running from CLion IDE, add ``--logtostderr=1`` to see the full output.
