================
Benchmark Driver
================

The benchmark driver can be used to measure the performance of queries in a
Presto cluster. We use it to continuously measure the performance of trunk.

Download :maven_download:`benchmark-driver`, rename it to ``presto-benchmark-driver``,
then make it executable with ``chmod +x``.

Suites
------

Create a ``suite.json`` file:

.. code-block:: json

    {
        "file_formats": {
            "query": ["single_.*", "tpch_.*"],
            "schema": [ "tpch_sf(?<scale>.*)_(?<format>.*)_(?<compression>.*?)" ],
            "session": {}
        },
        "legacy_orc": {
            "query": ["single_.*", "tpch_.*"],
            "schema": [ "tpch_sf(?<scale>.*)_(?<format>orc)_(?<compression>.*?)" ],
            "session": {
                "hive.optimized_reader_enabled": "false"
            }
        }
    }

This example contains two suites ``file_formats`` and ``legacy_orc``. The
``file_formats`` suite will run queries with names matching the regular expression
``single_.*`` or ``tpch_.*`` in all schemas matching the regular expression
``tpch_sf.*_.*_.*?``. The ``legacy_orc`` suite adds a session property to
disable the optimized ORC reader and only runs in the ``tpch_sf.*_orc_.*?``
schema.

Queries
-------

The SQL files are contained in a directory named ``sql`` and must have the
``.sql`` file extension. The name of the query is the name of the file
without the extension.

Output
------

The benchmark driver will measure the wall time, total CPU time used by
all Presto processes and the CPU time used by the query. For each timing, the
driver reports median, mean and standard deviation of the query runs. The
difference between process and query CPU times is the query overhead, which
is normally from garbage collections. The following is the output from the
``file_formats`` suite above:

.. code-block:: none

    suite        query          compression format scale wallTimeP50 wallTimeMean wallTimeStd processCpuTimeP50 processCpuTimeMean processCpuTimeStd queryCpuTimeP50 queryCpuTimeMean queryCpuTimeStd
    ============ ============== =========== ====== ===== =========== ============ =========== ================= ================== ================= =============== ================ ===============
    file_formats single_varchar none        orc    100   597         642          101         100840            97180              6373              98296           94610            6628
    file_formats single_bigint  none        orc    100   238         242          12          33930             34050              697               32452           32417            460
    file_formats single_varchar snappy      orc    100   530         525          14          99440             101320             7713              97317           99139            7682
    file_formats single_bigint  snappy      orc    100   218         238          35          34650             34606              83                33198           33188            83
    file_formats single_varchar zlib        orc    100   547         543          38          105680            103373             4038              103029          101021           3773
    file_formats single_bigint  zlib        orc    100   282         269          23          38990             39030              282               37574           37496            156

Note that the above output has been reformatted for readability from the
standard TSV that the driver outputs.

The driver can add additional columns to the output by extracting values from
the schema name or SQL files. In the suite file above, the schema names
contain named regular expression capturing groups for ``compression``,
``format``, and ``scale``, so if we ran the queries in a catalog containing the
schemas ``tpch_sf100_orc_none``, ``tpch_sf100_orc_snappy``, and
``tpch_sf100_orc_zlib``, we get the above output.

Another way to create additional output columns is by adding tags to the
SQL files. For example, the following SQL file declares two tags,
``projection`` and ``filter``:

.. code-block:: none

    projection=true
    filter=false
    =================
    SELECT SUM(LENGTH(comment))
    FROM lineitem

This will cause the driver to output these values for each run of this query.

CLI Arguments
-------------

The ``presto-benchmark-driver`` program contains many CLI arguments to control
which suites and queries to run, the number of warm-up runs and the number
of measurement runs. All of the command line arguments can be seen with the
``--help`` option.
