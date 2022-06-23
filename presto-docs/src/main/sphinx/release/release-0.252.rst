=============
Release 0.252
=============

**Highlights**
==============
* Add support for returning partial results.
* Add automatic query retry functionality for transient failures. 
* Add support for running base32 encoded ElasticSearch queries.

**Details**
===========

General Changes
_______________
* Fix the join operator to prevent hanging when spilling is enabled and the probe side finishes before the hash builder starts.
* Add new spilling strategy to spill when a combination of user, system and recovable memory on a node exceeds ``max_total_memory_per_node``. This can be enabled by setting the system configuration ``experimental.spiller.task-spilling-strategy`` to ``PER_QUERY_MEMORY_LIMIT``.
* Add support for logging formatted prepared query. This can be enabled by setting session property ``log_formatted_query_enabled`` to ``true``.
* Add support for returning partial results for the queries by setting ``partial_results_enabled`` session property. Additionally ``partial_results_max_execution_time_multiplier``, ``partial_results_completion_ratio_threshold`` session properties can be set to configure the max execution time multiplier and minimum completion ratio threshold for the queries.
* Add automatic query retry functionality for transient failures. This can be enabled by setting ``per-query-retry-limit`` to a non-zero integer to indicate the per query retry count.
* Add support to coordinator endpoint ``/v1/info/state`` to return ``ACTIVE`` when the coordinator is not shutting down and the cluster has the minimum required workers.
* Add functions :func:`chisquared_cdf` and :func:`inverse_chisquared_cdf`.

Security Changes
________________
* Add file based password authenticator plugin. See :doc:`/security/password-file`.

Hive Changes
____________
* Update Alluxio cache config property ``cache.alluxio.timeout-enabled`` to be ``true`` by default.

JDBC Changes
____________
* Add support to serialize ``PrestoArray`` to a string.

Elasticsearch Connector
_______________________
* Add support for running base32 encoded ElasticSearch queries. See :doc:`/connector/elasticsearch`.

Presto On Spark Changes
_______________________
* Improve bucketed table write parallelism.
* Fix a bug when Presto on Spark doesn't start because the temporary storage is not initialized.

**Contributors**
================

Ahmad Ghazal, Andrii Rosa, Arunachalam Thirupathi, Bhavani Hari, Bin Fan, Chen Li, Chi Tsai, Costin V Cozianu, Dongliang Chen, James Petty, James Sun, Junyi Huang, Ke Wang, Mayank Garg, Naveen Kumar Mahadevuni, Nikhil Collooru, Rebecca Schlussel, Rohit Jain, Rongrong Zhong, Shixuan Fan, Sreeni Viswanadha, Tal Galili, Tim Meehan, Vic Zhang, Zhenxiao Luo, imjalpreet, sophiashang, tanjialiang, v-jizhang
