=============
Release 0.225
=============

General Changes
_______________
* Fix under-reporting of output data size from a fragment. (:issue:`11770`)
* Fix a bug where spatial joins would cause workers to run out of memory and crash. (:pr:`13251`)
* Fix leak in operator peak memory computations. (:issue:`13210`)
* Allow overriding session zone sent by clients. (:issue:`13140`)
* Throw an exception when a spatial partition is made with 0 rows.
* Add ``TableStatistics`` field containing statistics estimates for input tables to the ``QueryInputMetadata`` in ``QueryCompletedEvent`` for join queries. (:pr:`12808`)
* Add configuration property ``experimental.max-total-running-task-count`` and ``experimental.max-query-running-task-count``
  to limit number of running tasks for all queries and a single query, respectively. Query will be killed only if both conditions are violated. (:pr:`13228`)

JDBC Changes
____________
* Match schema and table names case insensitively. This behavior can be enabled by setting the ``case-insensitive-name-matching`` catalog configuration option to true.

Web UI Changes
______________
* Display tasks per stage on the query page.

Elasticsearch Connector Changes
_______________________________
* Fix and support nested fields in Elasticsearch Connector. (:issue:`12642`)

Hive Changes
____________
* Improve performance of file reads on S3. (:pr:`13222`)
* Add ability to use JSON key file to access Google Cloud Storage. The file path can be specified using the configuration property ``hive.gcs.json-key-file-path=/path/to/gcs_keyfile.json``.
* Add ability to use client-provided OAuth token to access Google Cloud Storage. This can be configured using the configuration property ``hive.gcs.use-access-token=false``.

Raptor Changes
______________
* Return an error when creating tables with unsupported types like ROW type. (:pr:`13209`)
* Remove legacy ORC writer. Configuration property ``storage.orc.optimized-writer-stage`` is enabled by default. Option ``DISABLED`` is removed.
* Add a new procedure ``trigger_bucket_balancer`` to trigger bucket balancing job.

SPI Changes
___________
* Add ``TableHandle`` parameter to ``ConnectorPageSourceProvider#createPageSource`` method. This gives connector access to ``ConnectorTableLayoutHandle`` during execution.
* Add ``columnHandles`` parameter to ``ConnectorMetadata.getTableStatistics`` method. The new parameter allows connectors to prune statistics to the
  desired list of columns and subfields and fixes compatibility issue between subfield pruning and CBO. (:issue:`13082`)

Verifier Changes
________________
* Add support for overriding session properties for all queries in a suite.
* Add cluster type and retryable information for ``QueryFailure``.
* Add final query failure information to Verifier output event.
