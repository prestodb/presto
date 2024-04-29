==========
SHOW STATS
==========

Synopsis
--------

.. code-block:: none

    SHOW STATS FOR table
    SHOW STATS FOR ( SELECT * FROM table [ WHERE condition ] )
    SHOW STATS FOR ( SELECT col1, col2,... colN FROM table [ WHERE condition ] )

Description
-----------

``SHOW STATS FOR table``: Show statistics for a specific table.
It provides information about the number of rows, number of nulls, minimum and maximum values,
number of distinct values, and average length for each column in the table.

``SHOW STATS FOR ( query )``: Show statistics for the result of a specific query.
It provides the same information as ``SHOW STATS FOR table``, but for the result set of the query instead
of a single table.

Any statistics that are not populated or unavailable on the data source are returned as ``NULL``. Any additional
statistics collected on the data source other than those listed below are not included in the output of the
``SHOW STATS`` command.

The summary row does not correspond to any specific column so the ``column_name`` value for the summary row is ``NULL``.

The following table lists the returned columns and what statistics they represent.

.. list-table:: Statistics
  :widths: 20, 40, 40
  :header-rows: 1

  * - Column
    - Description
    - Notes
  * - ``column_name``
    - The name of the column
    - ``NULL`` in the table summary row.
  * - ``data_size``
    - The total size in bytes of all of the values in the column
    - ``NULL`` in the table summary row. Available for columns of string data types with variable widths.
  * - ``distinct_values_count``
    - The estimated number of distinct values in the column
    - ``NULL`` in the table summary row.
  * - ``nulls_fractions``
    - The portion of the values in the column that are ``NULL``
    - ``NULL`` in the table summary row.
  * - ``row_count``
    - The estimated number of rows in the table
    - ``NULL`` in column statistic rows.
  * - ``low_value``
    - The lowest value found in this column
    - ``NULL`` in the table summary row. Available for columns of DATE, integer, floating-point, and fixed-precision
      data types.
  * - ``high_value``
    - The highest value found in this column
    - ``NULL`` in the table summary row. Available for columns of DATE, integer, floating-point, and fixed-precision
      data types.
