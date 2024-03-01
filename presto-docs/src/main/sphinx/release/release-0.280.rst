=============
Release 0.280
=============

**Details**
===========

General Changes
_______________
* Fix a race condition, where if a query was submitted before the resource group configuration had been loaded by the Presto server, all queries to the cluster would fail with a ``NullPointerException``. 
* Fix a bug in the ``GatherAndMergeWindows`` optimization would produces invalid plans when the output of a window function was used in the frame definition of another window function.
* Fix the output of :func:`find_first` function for ``NULL`` Now it will throw exception if the found matched value is NULL.
* Fix a bug where duplicate items in ``UNNEST`` would lead to query compilation failure.
* Improve the column access checks for :func:`transform` and :func:`cardinality` functions, such that only the required subfields are checked.
* Improve filtering for large tables with ``LIMIT`` number of keys for queries that do simple ``GROUP BY LIMIT`` with no ``ORDER BY``. This feature can be enabled with a boolean session param: ``prefilter_for_groupby_limit``.
* Add :func:`remove_nulls` to remove null elements from the given array.
* Add function :func:`find_first_index` which returns the index of the first element which satisfies the condition.
* Add range with offset expression support to :doc:`/functions/window`.
* Add the names of the functions invoked by the query to the completed event. This can be enabled with the ``log_invoked_function_names_enabled`` session property or the ``log-invoked-function-names-enabled`` configuration property.
* Add a new runtime metric ``optimizerTimeNanos`` to measure the time taken by the optimizers. With this change the time taken by optimizers has been removed from runtime metric ``logicalPlannerTimeNanos``.
* Add support to report both client-side and server-side latency in presto-cli result summary. The client-side latency is the wall time presto-cli spends for the query to finish, and it no longer includes the time spent by the user paging through results. The server side latency represents the wall time the engine uses to finish executing this query.
* Add ANSI SQL compliant syntax ``FETCH FIRST N ROWS ONLY`` as an alternative way to express ``LIMIT n``. For example ``select * from table_name FETCH FIRST 3 ROWS ONLY`` is equivalent to ``SELECT * FROM table_name LIMIT 3``, and will return only 3 rows as a result of the sql query.
* Replace Airlift from version 0.205 to 0.207.
* Remove the ``deprecated.legacy-date-timestamp-to-varchar-coercion`` configuration property, which was used for Raptor to Presto migration.

Resource Groups Changes
_______________________
* Fix blocking resource group lock when query queue is full (``QUERY_QUEUE_FULL`` error)

Hive Changes
____________
* Add Amazon S3 Select pushdown for JSON files.

**Credits**
===========

Aditi Pandit, Ajay George, Amit Dutta, Anant Aneja, Arun Thirupathi, Avinash Jain, Ben Tony Joe, Chunxu Tang, Darren Fu, Deepak Majeti, Ge Gao, Ivan Sadikov, Jalpreet Singh Nanda (:imjalpreet), James Petty, Jingmei Huang, Karteek Murthy Samba Murthy, Konstantinos Karanasos, Linsong Wang, Lisheng, Maria Basmanova, Michael Shang, Miłosz Linkiewicz, Nikhil Collooru, Paul Meng, Pranjal Shankhdhar, Rebecca Schlussel, Rohit Jain, Ruslan Mardugalliamov, Sergey Pershin, Sergii Druzkin, Shrinidhi Joshi, Sreeni Viswanadha, Timothy Meehan, Vladimir Ozerov, Ying, Zac, Zhongjun Jin (Mark), dnnanuti, feilong-liu, singcha, tanjialiang, xiaoxmeng, 枢木
