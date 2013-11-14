======
SELECT
======

Synopsis
--------

.. code-block:: none

    [ WITH with_query [, ...] ]
    SELECT [ ALL | DISTINCT ] select_expr [, ...]
    [ FROM from_item [, ...] ]
    [ WHERE condition ]
    [ GROUP BY expression [, ...] ]
    [ HAVING condition]
    [ UNION [ ALL | DISTINCT ] select ]
    [ ORDER BY expression [ ASC | DESC ] [, ...] ]
    [ LIMIT count ]

where ``from_item`` is one of

.. code-block:: none

    table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]

.. code-block:: none

    from_item join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]

Description
-----------

Retrieve rows from zero or more tables.

TABLESAMPLE
-----------

Sample method can be specified as ``BERNOULLI`` or ``SYSTEM``.

Bernoulli method
	Each row is selected to be in the table sample with a probability of samplePercentage/100. When a table is sampled using the Bernoulli method, all physical blocks of the table are scanned and certain rows are skipped (based on the comparison between the samplePercentage and random value calculated at runtime). The probability of a row being included in the result is independent from any other row. This does not reduce the time required to read the sampled table from disk. It may have an impact on the total query time if the sampled output is processed further.

System method
	This sampling method divides the table into logical segments of data and samples the table at this granularity. This sampling method either selects all the rows from a particular segment of data or skips it (based on the comparison between the samplePercentage and random value calculated at runtime). The rows selected in a system sampling will be dependent on which connector is used. For example, when used with Hive, it is dependent on how the data is laid out on HDFS. This method does not guarantee independent sampling probabilities.

.. note:: Neither of the two methods allow deterministic bounds on the number of rows returned.

Example::

	SELECT *
	FROM dim_all_users
	TABLESAMPLE BERNOULLI (50);

	SELECT *
	FROM dim_all_users
	TABLESAMPLE SYSTEM (75);

Using with tablealias and joins::

	SELECT t1.*, t2.*
	FROM
	table1 t1 TABLESAMPLE SYSTEM (10)
	JOIN
	table2 t2 TABLESAMPLE BERNOULLI (40)
	ON
	t1.id = t2.id
