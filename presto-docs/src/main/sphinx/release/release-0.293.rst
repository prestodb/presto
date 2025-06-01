=============
Release 0.293
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fix ROLLBACK statement to ensure it successfully aborts non-auto commit transactions corrupted by failed statements. `#23247 <https://github.com/prestodb/presto/pull/23247>`_
* Fix Router Round robin scheduler candidate cluster index, by adding group specific index. `#24580 <https://github.com/prestodb/presto/pull/24580>`_
* Fix a bug in left join to semi join optimizer which leads to filter source variable not found error. `#25111 <https://github.com/prestodb/presto/pull/25111>`_
* Fix a bug where a mirrored :func:`arrays_overlap(x, y) -> boolean` function does not return the correct value. `#23845 <https://github.com/prestodb/presto/pull/23845>`_
* Fix returning incorrect results from the "second" UDF when a timestamp is in a time zone with an offset that is at the granularity of seconds. `#25090 <https://github.com/prestodb/presto/pull/25090>`_
* Fix the issue of sensitive data such as passwords and access keys being exposed in logs by redacting sensitive field values. `#24886 <https://github.com/prestodb/presto/pull/24886>`_
* Improve memory usage in reader with nested readers by resetting all nested readers. `#24912 <https://github.com/prestodb/presto/pull/24912>`_
* Improve ACL check by moving checkQueryIntegrity from Dispatch phase to Analyzer phase. `#24927 <https://github.com/prestodb/presto/pull/24927>`_
* Improve communication between coordinator and worker with thrift serde. `#25079 <https://github.com/prestodb/presto/pull/25079>`_
* Improve communication between coordinator and worker with thrift serde. `#25020 <https://github.com/prestodb/presto/pull/25020>`_
* Improve how we merge multiple operator stats together. `#24921 <https://github.com/prestodb/presto/pull/24921>`_
* Improve metrics creation by refactoring local variables to a dedicated class. `#24921 <https://github.com/prestodb/presto/pull/24921>`_
* Improve performance of ``ORDER BY`` queries on single node execution :pr:`25022`. `#25022 <https://github.com/prestodb/presto/pull/25022>`_
* Improve query plans using the ``SimplifyPlanWithEmptyInput`` optimizer to convert a table scan which returns no data to an empty values node. `#25155 <https://github.com/prestodb/presto/pull/25155>`_
* Add DDL SQL support for ``SHOW CREATE SCHEMA``. `#24356 <https://github.com/prestodb/presto/pull/24356>`_
* Add authentication capabilities to Presto router. `#24407 <https://github.com/prestodb/presto/pull/24407>`_
* Add configuration property ``hive.metastore.catalog.name`` to pass catalog names to the metastore, enabling catalog-based schema management and filtering. `#24235 <https://github.com/prestodb/presto/pull/24235>`_
* Add coordinator health checks to Presto router. `#24449 <https://github.com/prestodb/presto/pull/24449>`_
* Add counter JMX metrics to Presto router. `#24449 <https://github.com/prestodb/presto/pull/24449>`_
* Add example custom scheduler plugin - Metrics based custom scheduler plugin. `#24439 <https://github.com/prestodb/presto/pull/24439>`_
* Add support for custom scheduler plugin. `#24439 <https://github.com/prestodb/presto/pull/24439>`_
* Add type rewrite support for native execution. This feature can be enabled by ``native-execution-type-rewrite-enabled`` configuration property and ``native_execution_type_rewrite_enabled`` session property. :pr:`24916`. `#24916 <https://github.com/prestodb/presto/pull/24916>`_
* Add view definitions from Analyzer phase to perform full integrity check on query credentials. `#24955 <https://github.com/prestodb/presto/pull/24955>`_
* Replace ``exchange.compression-enabled``,  ``fragment-result-cache.block-encoding-compression-enabled``, ``experimental.spill-compression-enabled`` with ``exchange.compression-codec``, ``fragment-result-cache.block-encoding-compression-codec`` to enable compression codec configurations. Supported codecs include GZIP, LZ4, LZO, SNAPPY, ZLIB and ZSTD. `#24670 <https://github.com/prestodb/presto/pull/24670>`_
* Replace dependency from PostgreSQL to redshift-jdbc42 to address `CVE-2024-1597 <https://github.com/advisories/GHSA-24rp-q3w6-vc56>`_, `CVE-2022-31197 <https://github.com/advisories/GHSA-r38f-c4h4-hqq2>`_, and `CVE-2020-13692 <https://github.com/advisories/GHSA-88cc-g835-76rp>`_. `#25106 <https://github.com/prestodb/presto/pull/25106>`_
* Remove unused line of code from router module. `#25150 <https://github.com/prestodb/presto/pull/25150>`_
* Change checkQueryIntegrity function signature in AccessControl interface to pass in view definitions as params. `#24955 <https://github.com/prestodb/presto/pull/24955>`_
* Upgrade commons-compress version to 1.26.2 across the codebase to address `CVE-2021-35517 <https://github.com/advisories/GHSA-xqfj-vm6h-2x34>`_, `CVE-2021-35516 <https://github.com/advisories/GHSA-crv7-7245-f45f>`_, `CVE-2021-36090 <https://github.com/advisories/GHSA-mc84-pj99-q6hh>`_, `CVE-2021-35515 <https://github.com/advisories/GHSA-7hfm-57qf-j43q>`_, and `CVE-2024-25710 <https://github.com/advisories/GHSA-4g9r-vxhx-9pgx>`_. `#25106 <https://github.com/prestodb/presto/pull/25106>`_
* Upgrade kotlin-stdlib-jdk8 to 1.9.25. `#24971 <https://github.com/prestodb/presto/pull/24971>`_
* Upgrade netty version to 4.1.119.Final. `#24971 <https://github.com/prestodb/presto/pull/24971>`_
* Upgrade okio-jvm version to 3.9.1. `#24971 <https://github.com/prestodb/presto/pull/24971>`_
* Upgrade slf4j version to 1.7.36. `#24971 <https://github.com/prestodb/presto/pull/24971>`_
* Upgrade snappy-java version at 1.1.10.4 across the codebase to address `CVE-2023-43642 <https://github.com/advisories/GHSA-55g7-9cwv-5qfv>`_. `#25106 <https://github.com/prestodb/presto/pull/25106>`_


Prestissimo (Native Execution) Changes
______________________________________
* Fix REST API call ``v1/operator/task/getDetails?id=`` crash. `#24839 <https://github.com/prestodb/presto/pull/24839>`_
* Replace using native functions with Java functions for creating failure functions when native execution is enabled. `#24792 <https://github.com/prestodb/presto/pull/24792>`_
* Fix issue with PartitionAndSerialize re-using only keys from the first batch of data. `#25015 <https://github.com/prestodb/presto/pull/25015>`_
* Add BinarySortableSerializer tests with VectorFuzzer. `#24954 <https://github.com/prestodb/presto/pull/24954>`_
* Add runtime metrics collection for S3 Filesystem. `#24554 <https://github.com/prestodb/presto/pull/24554>`_
* Add supported for sort in PartitionAndSerialize operator. `#24953 <https://github.com/prestodb/presto/pull/24953>`_
* Remove worker configuration property ``register-test-functions``. `#24853 <https://github.com/prestodb/presto/pull/24853>`_

Security Changes
________________
* Add security-related headers to the static resources served from the Presto Router UI, including: ``Content-Security-Policy``, ``X-Content-Type-Options``. See reference docs `Content-Security-Policy <https://developer.mozilla.org/en-US/docs/Web/HTTP/CSP>`_ and  `X-Content-Type-Options <https://learn.microsoft.com/en-us/previous-versions/windows/internet-explorer/ie-developer/compatibility/gg622941(v=vs.85)>`_. :pr:`24272`. `#25165 <https://github.com/prestodb/presto/pull/25165>`_
* Add support for access control row filters and column masks on views. `#25052 <https://github.com/prestodb/presto/pull/25052>`_
* Add support for row filtering and column masking in access control. `#24277 <https://github.com/prestodb/presto/pull/24277>`_
* Upgrade commons-beanutils to version 1.9.4 in response to `CVE-2014-0114 <https://nvd.nist.gov/vuln/detail/CVE-2014-0114>`_. `#24665 <https://github.com/prestodb/presto/pull/24665>`_
* Upgrade plexus-utils to version 3.6.0 in response to `CVE-2017-1000487 <https://nvd.nist.gov/vuln/detail/cve-2017-1000487>`_. `#24665 <https://github.com/prestodb/presto/pull/24665>`_
* Upgrade zookeeper to 3.9.3 to fix security vulnerability in presto-accumulo, presto-delta,presto-hive,presto-kafka and presto-hudi  in response to `CVE-2023-44981 <https://nvd.nist.gov/vuln/detail/cve-2023-44981>`_. `#24403 <https://github.com/prestodb/presto/pull/24403>`_

Delta Lake Connector Changes
____________________________
* Fix a bug where after an incremental update with null values is made, reads start timing out. `#24920 <https://github.com/prestodb/presto/pull/24920>`_

Elasticsearch Connector Changes
_______________________________
* Upgrade elasticsearch to 7.17.27 in response to `CVE-2024-43709 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2024-43709>`_. `#23894 <https://github.com/prestodb/presto/pull/23894>`_

Hive Connector Changes
______________________
* Add support for Web Identity authentication in S3 security mapping with the ``hive.s3.webidentity.enabled`` property. `#24645 <https://github.com/prestodb/presto/pull/24645>`_
* Replace listObjects with listObjectsV2 in PrestoS3FileSystem listPrefix. `#24794 <https://github.com/prestodb/presto/pull/24794>`_

Iceberg Connector Changes
_________________________
* Fix to pass full session to avoid ``Unknown connector`` errors using the Nessie catalog. `#24803 <https://github.com/prestodb/presto/pull/24803>`_
* Add support for the procedure <catalog-name>.system.invalidate_manifest_file_cache() for ManifestFile cache invalidation in Iceberg. `#24831 <https://github.com/prestodb/presto/pull/24831>`_
* Add support for the procedure <catalog-name>.system.invalidate_statistics_file_cache() for StatisticsFile cache invalidation in Iceberg. `#24831 <https://github.com/prestodb/presto/pull/24831>`_
* Add support for bucket transform for columns of type ``TimeType`` in Iceberg table. `#24829 <https://github.com/prestodb/presto/pull/24829>`_
* Replace RowDelta with AppendFiles for insert-only statements such as INSERT and CTAS. `#24989 <https://github.com/prestodb/presto/pull/24989>`_

JDBC Connector Changes
______________________
* Add ``list-schemas-ignored-schemas`` configuration property for JDBC connectors. `#24994 <https://github.com/prestodb/presto/pull/24994>`_

Kafka Connector Changes
_______________________
* Add support for optional Apache Kafka SASL. `#24798 <https://github.com/prestodb/presto/pull/24798>`_

MongoDB Connector Changes
_________________________
* Add support for JSON type in MongoDB. `#25089 <https://github.com/prestodb/presto/pull/25089>`_

MySQL Connector Changes
_______________________
* Add support for GEOMETRY type in the MySQL connector. `#24996 <https://github.com/prestodb/presto/pull/24996>`_

SQL Server Connector Changes
____________________________
* Note: Starting from this version, the driver sets the encrypt property to ``true`` by default. To connect to a non-SSL SQL Server instance, you must set ``encrypt=false`` in your connection configuration to avoid connectivity issues. This is a breaking change for existing connections. `#24686 <https://github.com/prestodb/presto/pull/24686>`_
* Upgrade SQL Server driver to version 12.8.1 to support NTLM authentication. See :ref:`connector/sqlserver:authentication`. `#24686 <https://github.com/prestodb/presto/pull/24686>`_


Documentation Changes
_____________________
* Document :doc:`../presto_cpp/sidecar` and native sidecar plugin. `#24883 <https://github.com/prestodb/presto/pull/24883>`_

**Credits**
===========

Akinori Musha, Amit Dutta, Anant Aneja, Andrew Xie, Andrii Rosa, Anurag Dwivedi, Arjun Gupta, Bryan Cutler, Chen Yang, Christian Zentgraf, Deepak Majeti, Deepak Mehra, Denodo Research Labs, Elbin Pallimalil, Emily (Xuetong) Sun, Ethan Zhang, Facebook Community Bot, Feilong Liu, Gary Helmling, Haritha Koloth, Hazmi, HeidiHan0000, Heng Xiao, Jacob Khaliqi, James Petty, Jay Narale, Jim Simon, Jimmy Lu, Joe Abraham, Ke Wang, Ke Wang, Kevin Tang, Kevin Wilfong, Krishna Pai, Li Zhou, Linsong Wang, Mariam Almesfer, Miguel Blanco Godón, Najib Adan, Natasha Sehgal, Nidhin Varghese, Nikhil Collooru, Nivin C S, Pradeep Vaka, Pramod Satya, Prashant Golash, Pratik Joseph Dabre, Rebecca Schlussel, Reetika Agrawal, Samuel Majoros, Sayari Mukherjee, Serge Druzkin, Sergey Pershin, Shahim Sharafudeen, Shang Ma, Shelton Cai, Shijin, Steve Burnett, Tim Meehan, Xiao Du, Xiaoxuan Meng, Xin Zhang, Yihong Wang, Ying, Yuanda (Yenda) Li, Zac Blanco, Zac Wen, aditi-pandit, auden-woolfson, ebonnal, jp-sivaprasad, lukmanulhakkeem, mecit-san, mima0000, mohsaka, namya28, tanjialiang, vhsu14, wangd, wraymo
