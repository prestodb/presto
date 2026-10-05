=============
Release 0.300
=============

**Breaking Changes**
====================

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix ``REFRESH MATERIALIZED VIEW`` analysis to reject unsupported predicates nested within an ``OR``. `#28570 <https://github.com/prestodb/presto/pull/28570>`_
* Fix ``SHOW CREATE MATERIALIZED VIEW`` failure for materialized views created with materialized-view-only properties. `#28390 <https://github.com/prestodb/presto/pull/28390>`_
* Fix ``SHOW CREATE TABLE`` to not show hidden table properties for all existing connectors. `#28123 <https://github.com/prestodb/presto/pull/28123>`_
* Fix ``ST_Centroid`` to return ``NULL`` for empty geometries per ISO spec. `#26971 <https://github.com/prestodb/presto/pull/26971>`_
* Fix ``trim``, ``ltrim``, and ``rtrim`` functions for ``CHAR`` arguments to return unpadded ``VARCHAR`` results instead of padded ``CHAR`` values. `#28280 <https://github.com/prestodb/presto/pull/28280>`_
* Fix a query failure that could occur during planning for some queries with a distinct aggregation over an inner join. `#28231 <https://github.com/prestodb/presto/pull/28231>`_
* Fix failures when using an offset ``RANGE`` frame with ``ORDER BY`` inherited from a named window.  `#28517 <https://github.com/prestodb/presto/pull/28517>`_
* Fix fully qualified storage table names in materialized view queries. `#28423 <https://github.com/prestodb/presto/pull/28423>`_
* Fix query planning failure with dynamic filtering enabled when using scalar subqueries with ``min/max/count`` aggregation functions. `#28392 <https://github.com/prestodb/presto/pull/28392>`_
* Fix query runtime metrics being dropped for tables and materialized views referenced through a view. `#28367 <https://github.com/prestodb/presto/pull/28367>`_
* Fix the materialized view rewrite of a query that uses a window function or a ``WINDOW`` clause.  `#28473 <https://github.com/prestodb/presto/pull/28473>`_
* Improve ``SHOW CREATE TABLE`` to render the derived column information correctly. `#28123 <https://github.com/prestodb/presto/pull/28123>`_
* Improve accuracy of history based optimization statistics by recording them only from finished stages, skipping plan nodes which produced no output rows, and removing statistics of plan nodes of failed stages. `#28356 <https://github.com/prestodb/presto/pull/28356>`_
* Improve aggregation pushdown below an outer join when constraints, such as session property ``exploit_constraints=true``, can be used to establish that the outer side of a join is distinct. `#28425 <https://github.com/prestodb/presto/pull/28425>`_
* Improve property derivation to keep the probe side's local properties across a cross join whose build side produces at most one row, instead of discarding them. Operators above such a join, such as window functions and streaming aggregations, can now use the order of the probe input. `#28371 <https://github.com/prestodb/presto/pull/28371>`_
* Improve query planning performance by resolving window-value functions lazily during expression optimization. `#28265 <https://github.com/prestodb/presto/pull/28265>`_
* Improve the ``optimize_join_fan_out`` optimization to collapse the null-supplying side of an outer join, and to detect the fan-out from the grouping a join input reports rather than from a fixed plan shape. `#28301 <https://github.com/prestodb/presto/pull/28301>`_
* Add :func:`!xxhash128` function to compute the 128-bit XXH3 hash. `#28289 <https://github.com/prestodb/presto/pull/28289>`_
* Add Basic authentication support to the Prometheus connector. `#28391 <https://github.com/prestodb/presto/pull/28391>`_
* Add SQL Parser support for ``CREATE TABLE`` and ``ALTER TABLE`` to allow for derived column syntax. `#28123 <https://github.com/prestodb/presto/pull/28123>`_
* Add ``ALTER TABLE ... ALTER COLUMN <column> FIRST | AFTER <column>`` to move an existing column within a table's column order. Reordering columns requires connector support. `#28349 <https://github.com/prestodb/presto/pull/28349>`_
* Add ``spark.max-splits-count-per-partition`` configuration property and ``max_splits_count_per_spark_partition`` session property to bound the number of splits assigned to a single Spark input partition. Unbounded by default. `#28326 <https://github.com/prestodb/presto/pull/28326>`_
* Add an optional ``FIRST`` or ``AFTER <column>`` clause to ``ALTER TABLE ... ADD COLUMN`` to control where the new column is placed. Omitting the clause appends the column, which is the previous behavior. ``FIRST`` and ``AFTER`` require connector support. `#28328 <https://github.com/prestodb/presto/pull/28328>`_
* Add opt-in coordinator query tracing to runtime statistics with the ``runtime_stats_tracing_enabled`` session property. `#28417 <https://github.com/prestodb/presto/pull/28417>`_
* Add session property ``rewrite_approx_distinct_if_to_mask`` and configuration property ``optimizer.rewrite-approx-distinct-if-to-mask``, disabled by default, which rewrites ``approx_distinct(IF(p, e))`` to ``approx_distinct(e)`` masked by ``p``. `#28363 <https://github.com/prestodb/presto/pull/28363>`_
* Add support for declaring field names in a row constructor, for example ``ROW(1 AS a, 2 AS b)``. `#28298 <https://github.com/prestodb/presto/pull/28298>`_
* Add support for fractional native RPC admission and adaptive pacing. `#28573 <https://github.com/prestodb/presto/pull/28573>`_
* Add support for loading expression optimizers on the Presto on Spark driver from configuration supplied at startup, for deployments that have no ``etc/expression-manager/`` directory on local disk. Previously such deployments could register an expression optimizer factory but never instantiate it. `#28393 <https://github.com/prestodb/presto/pull/28393>`_
* Add support for the ``WINDOW`` clause, so a window specification can be named once and reused, for example ``SELECT rank() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)``. `#28471 <https://github.com/prestodb/presto/pull/28471>`_
* Remove support for ANSI SQL syntax in the ``trim`` function, which made ``TRIM``, ``BOTH``, ``LEADING``, and ``TRAILING`` reserved keywords. `#28334 <https://github.com/prestodb/presto/pull/28334>`_
* Update ``VALUES`` to derive column names from row field names, so ``SELECT a FROM (VALUES ROW(1 AS a))`` resolves. Previously such columns were named ``_col0``. `#28298 <https://github.com/prestodb/presto/pull/28298>`_

Prestissimo (Native Execution) Changes
______________________________________
* Fix Arrow connector column projection when there are duplicate column names. `#28462 <https://github.com/prestodb/presto/pull/28462>`_
* Fix ``NOT NULL`` column constraints being ignored for table writes, which allowed an ``INSERT`` to silently write ``NULL`` into a ``NOT NULL`` column. `#28450 <https://github.com/prestodb/presto/pull/28450>`_
* Fix the propagation of structured execution failure information from the Flight shim server through the Arrow Flight federation connector back to the Presto coordinator, retaining original exception type and error code. `#28126 <https://github.com/prestodb/presto/pull/28126>`_
* Add ``asyncDataCacheBytes`` and ``queryMemoryBytes`` to the native worker ``/v1/status`` endpoint to report async data cache memory and query memory separately. `#27783 <https://github.com/prestodb/presto/pull/27783>`_
* Add the ``exchange.materialization.use-zero-copy-collect`` configuration property to allow chain-capable shuffle backends to avoid an intermediate coalescing copy. This property is enabled by default and can be set to false to restore the contiguous path. `#27981 <https://github.com/prestodb/presto/pull/27981>`_
* Add ``sidecar.retry.max-failure-interval`` exponential-backoff retry configuration property for all native sidecar HTTP calls so that transient sidecar failures are retried for up to 5 seconds before propagating as a query failure. `#28429 <https://github.com/prestodb/presto/pull/28429>`_
* Add session property :ref:`presto_cpp/properties-session:\`\`native_abandon_partial_topn_row_number_min_pct\`\`` which sets the percentage of accumulated input rows still retained at or above which the partial ``TopNRowNumber`` operator is abandoned. Defaults to ``80``, the existing Velox default, so behavior is unchanged unless set. `#28314 <https://github.com/prestodb/presto/pull/28314>`_
* Add session property :ref:`presto_cpp/properties-session:\`\`native_abandon_partial_topn_row_number_min_rows\`\`` which sets the number of input rows the partial ``TopNRowNumber`` operator accumulates before checking whether to abandon it. Defaults to ``100000``, the existing Velox default, so behavior is unchanged unless set. `#28314 <https://github.com/prestodb/presto/pull/28314>`_

Security Changes
________________
* Remove the ``kotlin-stdlib-jdk8`` dependency, which reached end-of-life as of Kotlin 1.8.0 and has been merged into ``kotlin-stdlib``. See `Kotlin 1.8.0 release notes <https://kotlinlang.org/docs/whatsnew18.html#updated-jvm-compilation-target>`_. `#28504 <https://github.com/prestodb/presto/pull/28504>`_
* Upgrade Netty to 4.2.17.Final to address `CVE-2026-59902 <https://github.com/advisories/GHSA-2qj4-mmr9-4v2f>`_ and `CVE-2026-59903 <https://github.com/advisories/GHSA-8c42-7qj2-3j46>`_ . `#28419 <https://github.com/prestodb/presto/pull/28419>`_
* Upgrade ``httpclient5`` to 5.6.4 in response to `CVE-2026-64607 <https://github.com/advisories/GHSA-hjcp-jmpx-g3qm>`_. `#28472 <https://github.com/prestodb/presto/pull/28472>`_
* Upgrade async-http-client to 3.0.13 to address `CVE-2026-85716 <https://github.com/advisories/GHSA-fj9w-c36g-h5x8>`_ , `CVE-2026-85717 <https://github.com/advisories/GHSA-f8m2-889x-vw4x>`_ , `CVE-2026-85718 <https://www.cve.org/CVERecord?id=CVE-2026-85718>`_ , `CVE-2026-85719 <https://www.cve.org/CVERecord?id=CVE-2026-85719>`_ , `CVE-2026-85720 <https://github.com/advisories/GHSA-xr57-gcx8-52hf>`_ , `CVE-2026-85721 <https://github.com/advisories/GHSA-7grg-jcf7-rpmx>`_. `#28521 <https://github.com/prestodb/presto/pull/28521>`_
* Upgrade bouncycastle version to 1.85 to address multiple CVEs. `#28436 <https://github.com/prestodb/presto/pull/28436>`_
* Upgrade jetty to 12.0.38 to address `CVE-2026-10050 <https://github.com/advisories/GHSA-2fvj-hgj9-j2gr>`_ , `CVE-2026-10051 <https://github.com/advisories/GHSA-f4v5-65jj-pcr2>`_ , `CVE-2026-6790 <https://github.com/advisories/GHSA-7p3p-8qv8-m2vh>`_ ,  `CVE-2026-8384 <https://github.com/advisories/GHSA-w7x5-g22v-xqhr>`_. `#28320 <https://github.com/prestodb/presto/pull/28320>`_
* Upgrade libthrift to 0.24.0 to address `CVE-2026-41608 <https://github.com/advisories/GHSA-6pjx-3pjc-mrj8>`_ , `CVE-2026-43871 <https://github.com/advisories/GHSA-8wv5-x4w7-5gww>`_ , `CVE-2026-45112 <https://github.com/advisories/GHSA-g9fj-fh28-776p>`_ , `CVE-2026-48144 <https://github.com/advisories/GHSA-367h-9jj5-w29f>`_ , `CVE-2026-48145 <https://github.com/advisories/GHSA-p2c7-c4gw-678h>`_ , `CVE-2026-48586 <https://github.com/advisories/GHSA-6h9h-5c4r-fqgf>`_ , `CVE-2026-49158 <https://github.com/advisories/GHSA-jcj7-w43p-gjh8>`_ , `CVE-2026-55968 <https://github.com/advisories/GHSA-8cj2-994r-9fpq>`_ , `CVE-2026-55969 <https://github.com/advisories/GHSA-x8jh-qfcv-fp29>`_ , `CVE-2026-55970 <https://github.com/advisories/GHSA-g7xp-pggm-c7w2>`_ , `CVE-2026-55971 <https://github.com/advisories/GHSA-gwj4-q93m-wwqc>`_ , `CVE-2026-58023 <https://github.com/advisories/GHSA-7rjc-4234-rgwm>`_ , `CVE-2026-58389 <https://github.com/advisories/GHSA-jx7g-767m-86hp>`_ , `CVE-2026-58662 <https://github.com/advisories/GHSA-x4w6-p6f8-24qw>`_ , `CVE-2026-66053 <https://github.com/advisories/GHSA-hwrj-9rr4-24xh>`_. `#28242 <https://github.com/prestodb/presto/pull/28242>`_
* Upgrade mariadb-java-client to 3.5.9 to address `CVE-2026-55856 <https://github.com/advisories/GHSA-g9jj-cgmh-9f38>`_, `CVE-2026-55857 <https://github.com/advisories/GHSA-qxvw-fvwx-5cp7>`_, and `CVE-2026-55858 <https://github.com/advisories/GHSA-xvr9-35cr-46v9>`_. `#28406 <https://github.com/prestodb/presto/pull/28406>`_
* Upgrade postgresql to 42.7.12 to address `CVE-2026-54291  <https://github.com/advisories/GHSA-j92g-9f8w-j867>`_. `#28203 <https://github.com/prestodb/presto/pull/28203>`_
* Upgrade the MongoDB Java driver to 5.12.0 to address `CVE-2026-88033 <https://nvd.nist.gov/vuln/detail/CVE-2026-88033>`_ and `CVE-2026-18710 <https://nvd.nist.gov/vuln/detail/CVE-2026-18710>`_. `#28537 <https://github.com/prestodb/presto/pull/28537>`_
* Upgrade the ZooKeeper dependency to 3.9.6 to remediate `CVE-2026-59969 <https://github.com/advisories/GHSA-4pv3-cr63-m4ph>`_ , `CVE-2026-59739 <https://github.com/advisories/GHSA-wrgg-c9rw-j78f>`_ , `CVE-2026-79993 <https://github.com/advisories/GHSA-9x9g-5r48-3wvg>`_ , `CVE-2026-84439 <https://github.com/advisories/GHSA-mxvw-9w3g-hmrq>`_ , and `CVE-2026-84501 <https://github.com/advisories/GHSA-mm24-r27q-hxg5>`_ . `#28558 <https://github.com/prestodb/presto/pull/28558>`_

JDBC Driver Changes
___________________
* Add JDBC metadata cache within transactions. `#28402 <https://github.com/prestodb/presto/pull/28402>`_

Elasticsearch Connector Changes
_______________________________
* Fix query failures with ``Not in GZIP format`` when Elasticsearch returns compressed responses. `#28472 <https://github.com/prestodb/presto/pull/28472>`_
* Upgrade ``elasticsearch-java`` to 9.5.3. `#28472 <https://github.com/prestodb/presto/pull/28472>`_

Hive Connector Changes
______________________
* Upgrade hudi-presto-bundle to version 1.1.0. **NOTE**: Partitioned Hudi 1.x tables require ``hive.parquet.use-column-names=true`` for correct column resolution. Without this setting, columns are read positionally and may return incorrect data. `#27274 <https://github.com/prestodb/presto/pull/27274>`_

Hudi Connector Changes
______________________
* Upgrade hudi-presto-bundle to version 1.1.0. `#27274 <https://github.com/prestodb/presto/pull/27274>`_

Iceberg Connector Changes
_________________________
* Fix Iceberg Parquet nested dereference pushdown for types that are unsupported or lossy in Hive. `#28512 <https://github.com/prestodb/presto/pull/28512>`_
* Fix Invalid view JSON error when listing or querying views in an Iceberg REST catalog that were created by a non-Presto engine such as Netezza, Spark, or Trino. `#28321 <https://github.com/prestodb/presto/pull/28321>`_
* Fix ``Class com.facebook.presto.hive.s3.PrestoS3FileSystem not found`` failure when reading a newly registered Iceberg table stored in S3 with ``iceberg.pushdown-filter-enabled`` enabled. `#28486 <https://github.com/prestodb/presto/pull/28486>`_
* Fix table statistics ignoring a filter that was pushed into the table scan, which made the estimated row count that of the whole table and could lead to a worse plan. `#28549 <https://github.com/prestodb/presto/pull/28549>`_
* Fix: users can now create materialized views with the same name in different schemas when ``iceberg.materialized-view-default-storage-schema`` is configured, without hitting a ``Table already exists`` error. `#28438 <https://github.com/prestodb/presto/pull/28438>`_
* Add ``presto.derived-columns.spec.json`` table property, to store derived column specs as embedded json. `#28123 <https://github.com/prestodb/presto/pull/28123>`_
* Add support for ``ALTER TABLE ... ALTER COLUMN <column> FIRST | AFTER <column>``, which moves an existing column within the table's column order. `#28349 <https://github.com/prestodb/presto/pull/28349>`_
* Add support for reading Variant columns as JSON columns. `#28469 <https://github.com/prestodb/presto/pull/28469>`_
* Add support for the ``FIRST`` and ``AFTER <column>`` clauses of ``ALTER TABLE ... ADD COLUMN``, which control where the new column is placed in the table's column order. `#28328 <https://github.com/prestodb/presto/pull/28328>`_
* Add that when add, delete, rename or set type is performed on a column that Presto maintains the derived column information in sync. `#28123 <https://github.com/prestodb/presto/pull/28123>`_

Lance Connector Changes
_______________________
* Fix ``SHOW TABLES`` and ``SHOW SCHEMAS`` silently returning only the first page of results for catalogs larger than the namespace server's page size. `#28267 <https://github.com/prestodb/presto/pull/28267>`_

Oracle Connector Changes
________________________
* Fix the Oracle connector ignoring per-session ``user-credential-name`` and ``password-credential-name``extraCredentials, always using the static ``connection-user`` and ``connection-password`` instead. `#28351 <https://github.com/prestodb/presto/pull/28351>`_

Verifier Changes
________________
* Fix function call substitution failing when the substitute declares more window fields than the call it matched. This is now treated as a non match. `#28473 <https://github.com/prestodb/presto/pull/28473>`_
* Fix function call substitution silently dropping a named window. A substitution is no longer applied to a call whose window refers to a name declared in a ``WINDOW`` clause, because the substitution cannot see the partitioning, ordering or frame that name carries. `#28473 <https://github.com/prestodb/presto/pull/28473>`_
* Improve ``SCHEMA_MISMATCH`` failure reports to identify the columns that differ. `#28440 <https://github.com/prestodb/presto/pull/28440>`_
* Add validation rejecting a function call substitute whose window refers to a named window, for example ``rank() over w``. A substitute is parsed on its own, so the name has nothing to resolve against. `#28473 <https://github.com/prestodb/presto/pull/28473>`_

SPI Changes
___________
* Add ``setColumnPosition`` to ``ConnectorMetadata``, which moves an existing column within a table's column order. The default implementation reports that the connector does not support moving columns. `#28349 <https://github.com/prestodb/presto/pull/28349>`_
* Add an ``addColumn`` overload to ``ConnectorMetadata`` that takes a ``ColumnPosition``. The default implementation delegates to the existing three-argument ``addColumn`` when the column is appended, so connectors need no changes; override it to support the ``FIRST`` and ``AFTER`` clauses. `#28328 <https://github.com/prestodb/presto/pull/28328>`_

Documentation Changes
_____________________
* Add :doc:`/clients/adbc` to :doc:`/clients`. `#28383 <https://github.com/prestodb/presto/pull/28383>`_

**Credits**
===========

Aditi Pandit, Adrian Carpente (Denodo), Ajay Kharat, Amit Dutta, Anant Aneja, Andrii Rosa, Apurva Kumar, Arjun Gupta, Auden Woolfson, Bryan Cutler, Chandrakant Vankayalapati, Chandrashekhar Kumar Singh, Christian Zentgraf, Deepak Majeti, Deepak Mehra, Deepthi Bose, Denis Krivenko, Dilli Babu Godari, Dong Wang, Ge Gao, Hani Damlaj, Harsh Chokshi, Jacob GLZ, Jalpreet Singh Nanda, Jay Narale, Jeremy G, Jianjian Xie, Joe Abraham, Julian Rossi, KNagaVivek, Kevin Tang, Luis Garcés-Erice, Maria Basmanova, Matt Karrmann, Matthias Braun, Nandakumar Balagopal, Natasha Sehgal, Naveen Mahadevuni, Nishitha K Bhaskaran, Nivin C S, Prashant Sharma, Pratik Joseph Dabre, Pratyaksh Sharma, Reetika Agrawal, RindsSchei225e, Sayari Mukherjee, Shahim Sharafudeen, Shakyan Kushwaha, Shreya, Shrinidhi Joshi, Sreeni Viswanadha, Steve Burnett, Timothy Meehan, Tirumala Saiteja Goruganthu, XiaoDu, Yabin Ma, Yihong Wang, Ying, abhash, abhinavmuk04, adheer-araokar, bibith4, carlnayak, dependabot[bot], jkhaliqi, karthikchundi-commits, mohsaka, zhichenxu-meta
