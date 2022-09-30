================
Window functions
================

Velox window functions can be used to compute SQL window functions.

Understanding the window function definition
--------------------------------------------
Window functions operate over all the input rows.

Each window function can be thought to operate with an OVER clause
that specifies how it is evaluated:

.. code-block::

   function(args) OVER (
        [PARTITION BY expression]
        [ORDER BY expression [ASC|DESC]]
        [frame]
   )

A frame is one of:

.. code-block::

   {RANGE|ROWS} frame_start
   {RANGE|ROWS} BETWEEN frame_start AND frame_end

frame_start and frame_end can be any of:

.. code-block::

   UNBOUNDED PRECEDING
   expression PRECEDING  -- only allowed in ROWS mode
   CURRENT ROW
   expression FOLLOWING  -- only allowed in ROWS mode
   UNBOUNDED FOLLOWING

More details:

* The PARTITION BY fields separate the input rows into different partitions.

  This is analogous to how the aggregate functions input is separated into different groups for evaluation.
  If PARTITION BY fields are not specified, the entire input is treated as a single partition.

* The ORDER BY fields determine the order in which input rows are processed by the window function.

  If ORDER BY fields are not specified, the ordering is undefined.

* The frame clause specifies the sliding window of rows to be processed by the function for a given input row.

  A frame can be ROWS type or RANGE type, and it runs from frame_start to frame_end.
  If frame_end is not specified, a default value of CURRENT ROW is used.

  In ROWS mode, CURRENT ROW refers to the current row.

  In RANGE mode, CURRENT ROW refers to any peer row of the current row.
  Rows are peers if they have the same values for the ORDER BY fields.
  A frame start of CURRENT ROW refers to the first peer row of the current row,
  while a frame end of CURRENT ROW refers to the last peer row of the current row.
  If no ORDER BY is specified, all rows are considered peers of the current row.

  If no frame is specified, a default frame of RANGE UNBOUNDED PRECEDING is used.

SQL Example
___________

Window functions can be used to evaluate the following SQL query.
The query ranks orders for each clerk by price:

.. code-block:: sql

   SELECT orderkey, clerk, totalprice,
          rank() OVER (PARTITION BY clerk ORDER BY totalprice DESC) AS rnk
   FROM orders ORDER BY clerk, rnk;

=================
Ranking functions
=================

.. function:: dense_rank() -> bigint

Returns the rank of a value in a group of values. This is similar to rank(), except that tie values do
not produce gaps in the sequence.

.. function:: percent_rank() -> double

Returns the percentage ranking of a value in a group of values. The result is ``(r - 1) / (n - 1)`` where ``r``
is the ``rank()`` of the row and ``n`` is the total number of rows in the window partition.

.. function:: rank() -> bigint

Returns the rank of a value in a group of values. The rank is one plus the number of rows preceding the
row that are not peer with the row. Thus, the values in the ordering will produce gaps in the sequence.
The ranking is performed for each window partition.

.. function:: row_number() -> bigint

Returns a unique, sequential number for each row, starting with one, according to the ordering of rows
within the window partition.
