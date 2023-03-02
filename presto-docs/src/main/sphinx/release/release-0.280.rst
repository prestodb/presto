=============
Release 0.280
=============

**Details**
===========

General Changes
_______________
* Fix a bug where if a query was submitted before the resource group configuration had been loaded, all queries to the cluster would fail with a ``NullPointerException``.
* Fix bug in ``GatherAndMergeWindows`` optimization rule.
* Fix output of :func:`find_first` function for ``NULL`` Now it will throw exception if the found matched value is NULL.
* Fix the bug where duplicate items in ``UNNEST`` leads to query compilation failure.
* Add :func:`remove_nulls` to remove null elements from the given array.
* Add UDF `find_first_index` Returns the index of the first element which satisfies the condition.
* Add range with offset expression support to [window function](https://prestodb.io/docs/current/functions/window.html?highlight=window#functions-window--page-root).
* Add the names of the functions invoked by the query to the completed event. This can be enabled with the ``log_invoked_function_names_enabled`` session property or the ``log-invoked-function-names-enabled`` configuration property.
* Added a new optimization for filtering large tables with ``LIMIT`` number of keys for queries that do simple ``GROUP BY LIMIT`` with no ``ORDER BY``. Added a boolean session param: ``prefilter_for_groupby_limit`` that can enable this feature.
* Added a new runtime metric ``optimizerTimeNanos`` to measure time taken by optimizers. The time taken by optimizers has been removed from runtime metric ``logicalPlannerTimeNanos``.
* Added a pluggable ``QueryAnalyzer``  interface for analyzing queries and creating logical plans. This interface can be implemented to add additional analyzers.
* Report both client-side and server-side latency in presto-cli result summary. With this change, the client-side latency is the wall time presto-cli spends for this query to finish, but no longer includes the time spent by user paging through results. The server side latency is acquired from the Presto server which represents the wall time the engine uses to finish executing this query.
* Upgrade Airlift to 0.207.
* Use less restrictive column access checks when using :func:`transform` and :func:`cardinality` functions.

Resource Groups Changes
_______________________
* Fix blocking resource groups when query queue full.

Hive Changes
____________
* Add Amazon S3 Select pushdown for JSON files.

**Credits**
===========

Aditi Pandit, Ajay George, Amit Dutta, Anant Aneja, Arun Thirupathi, Avinash Jain, Ben Tony Joe, Chunxu Tang, Darren Fu, Deepak Majeti, Ge Gao, Ivan Sadikov, Jalpreet Singh Nanda (:imjalpreet), James Petty, Jingmei Huang, Karteek Murthy Samba Murthy, Konstantinos Karanasos, Linsong Wang, Lisheng, Maria Basmanova, Michael Shang, Miłosz Linkiewicz, Nikhil Collooru, Paul Meng, Pranjal Shankhdhar, Rebecca Schlussel, Rohit Jain, Ruslan Mardugalliamov, Sergey Pershin, Sergii Druzkin, Shrinidhi Joshi, Sreeni Viswanadha, Timothy Meehan, Vladimir Ozerov, Ying, Zac, Zhongjun Jin (Mark), dnnanuti, feilong-liu, singcha, tanjialiang, xiaoxmeng, 枢木
