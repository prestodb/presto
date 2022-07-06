Velox contains a drop-in copy of [DuckDB](https://duckdb.org/) code. It is
used in tests as a reference in-memory database to check results of Velox
evaluation for correctness. If you need to update it to pick up a bug fix or
a new feature, first clone DuckDB git repository:

    git clone https://github.com/cwida/duckdb.git
    cd duckdb/

Then generate the amalgamated .cpp and .hpp files:

    python3 scripts/amalgamation.py --extended --splits=8
    python3 scripts/parquet_amalgamation.py

Then copy the generated files to velox/external/duckdb:

    export VELOX_PATH="<path/to/velox>"
    rsync -vrh src/amalgamation/duckdb* ${VELOX_PATH}/velox/external/duckdb/
    rsync -vrh src/amalgamation/parquet* ${VELOX_PATH}/velox/external/duckdb/

We also maintain a copy of TPC-H dataset generators that need to be updated:

    rsync -vrh --exclude={'CMakeLists.txt','LICENSE','*.py','dbgen/queries','dbgen/answers'} extension/tpch/ ${VELOX_PATH}/velox/external/duckdb/tpch/

After the new files are copied, ensure that the new code compiles and that it
doesn't break any tests. Velox relies on many internal APIs, so there is a good
chance that this will not work out-of-the-box and that you will have to dig in
to find out what is wrong.

Once everything works, submit a PR.
