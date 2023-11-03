================
Window Functions
================

Window functions perform calculations across rows of the query result.
They run after the ``HAVING`` clause but before the ``ORDER BY`` clause.
Invoking a window function requires special syntax using the ``OVER``
clause to specify the window as follows::

    function(args) OVER (
        [PARTITION BY expression]
        [ORDER BY expression [ASC|DESC]]
        [frame]
    )

A ``frame`` is one of::

    {RANGE|ROWS|GROUPS} frame_start
    {RANGE|ROWS|GROUPS} BETWEEN frame_start AND frame_end

``frame_start`` and ``frame_end`` can be any of::

    UNBOUNDED PRECEDING
    expression PRECEDING
    CURRENT ROW
    expression FOLLOWING
    UNBOUNDED FOLLOWING


The window definition has 3 components:

* The ``PARTITION BY`` clause separates the input rows into different
  partitions. This is analogous to how the ``GROUP BY`` clause separates rows
  into different groups for aggregate functions.  If ``PARTITION BY`` is not
  specified, the entire input is treated as a single partition.
* The ``ORDER BY`` clause determines the order in which input rows
  will be processed by the window function.  If ``ORDER BY`` is not specified,
  the ordering is undefined.
  **Note that the ORDER BY clause within window functions does not support ordinals. You need to use actual expressions**
* The ``frame`` clause specifies the sliding window of rows to be processed by the
  function for a given input row.  A frame can be ``ROWS`` type, ``RANGE`` type or ``GROUPS`` type,
  and it runs from ``frame_start`` to ``frame_end``. If ``frame_end`` is not specified,
  a default value of ``CURRENT ROW`` is used.

  In ``ROWS`` mode, ``CURRENT ROW`` refers specifically to the current row. In ``RANGE`` and ``GROUPS``
  mode, ``CURRENT ROW`` refers to any peer row of the current row for the purpose
  of the ``ORDER BY``. If no ``ORDER BY`` is specified, all rows are considered peers
  of the current row. In ``RANGE`` and ``GROUPS`` mode a frame start of ``CURRENT ROW`` refers to
  the first peer row of the current row, while a frame end of ``CURRENT ROW`` refers to
  the last peer row of the current row.

  In ``ROWS`` mode, frame starts and ends of ``expression PRECEDING`` or ``expression FOLLOWING``
  define the start or end of the frame as the specified number
  of rows before or after the current row. The ``expression`` must be of type ``INTEGER``.

  In ``RANGE`` mode, frame starts and ends of ``expression PRECEDING`` or ``expression FOLLOWING``
  define the start or end of the frame as the value difference of the sort key from
  the current row. The sort key must either be the same type of ``expression`` or can be coerced to the
  same type as ``expression``.

  In ``GROUPS`` mode, frame starts and ends of ``expression PRECEDING`` or ``expression FOLLOWING``
  define the start or end of the frame as the number of groups from the current row.
  A group includes all rows with the same value on the sort key.
  The type of ``expression`` must be INTEGER or BIGINT.

  If no frame is specified, a default frame of ``RANGE UNBOUNDED PRECEDING`` is used.

Examples
--------

The following query ranks orders for each clerk by price::

    SELECT orderkey, clerk, totalprice,
           rank() OVER (PARTITION BY clerk
                        ORDER BY totalprice DESC) AS rnk
    FROM orders
    ORDER BY clerk, rnk

The following queries demonstrate the difference between ``ROWS``, ``RANGE`` and ``GROUPS`` in frame definition::

    SELECT
        ARRAY_AGG(v) OVER (
            ORDER BY k ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )
    FROM (
        VALUES (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd'), (5, 'e')
    ) t(k, v); -- ['a', 'b'], ['a', 'b', 'c'], ['b', 'c', 'd'], ['c', 'd', 'e'], ['d', 'e']

    SELECT
        ARRAY_AGG(v) OVER (
            ORDER BY k ASC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )
    FROM (
        VALUES (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd'), (5, 'e')
    ) t(k, v); -- ['a', 'b'], ['a', 'b'], ['c', 'd'], ['c', 'd'], ['e']

    SELECT
        ARRAY_AGG(v) OVER (
            ORDER BY k ASC GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )
    FROM (
        VALUES (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd'), (5, 'e')
    ) t(k, v); -- ['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd', 'e'], ['a', 'b', 'c', 'd', 'e'], ['c', 'd', 'e']

Aggregate Functions
-------------------

All :doc:`aggregate` can be used as window functions by adding the ``OVER``
clause. The aggregate function is computed for each row over the rows within
the current row's window frame.

For example, the following query produces a rolling sum of order prices
by day for each clerk::

    SELECT clerk, orderdate, orderkey, totalprice,
           sum(totalprice) OVER (PARTITION BY clerk
                                 ORDER BY orderdate) AS rolling_sum
    FROM orders
    ORDER BY clerk, orderdate, orderkey

Ranking Functions
-----------------

.. function:: cume_dist() -> double

    Returns the cumulative distribution of a value in a group of values.
    The result is the number of rows preceding or peer with the row in the
    window ordering of the window partition divided by the total number of
    rows in the window partition. Thus, any tie values in the ordering will
    evaluate to the same distribution value.

.. function:: dense_rank() -> bigint

    Returns the rank of a value in a group of values. This is similar to
    :func:`rank`, except that tie values do not produce gaps in the sequence.

.. function:: ntile(n) -> bigint

    Divides the rows for each window partition into ``n`` buckets ranging
    from ``1`` to at most ``n``. Bucket values will differ by at most ``1``.
    If the number of rows in the partition does not divide evenly into the
    number of buckets, then the remainder values are distributed one per
    bucket, starting with the first bucket.

    For example, with ``6`` rows and ``4`` buckets, the bucket values would
    be as follows: ``1`` ``1`` ``2`` ``2`` ``3`` ``4``

.. function:: percent_rank() -> double

    Returns the percentage ranking of a value in group of values. The result
    is ``(r - 1) / (n - 1)`` where ``r`` is the :func:`rank` of the row and
    ``n`` is the total number of rows in the window partition.

.. function:: rank() -> bigint

    Returns the rank of a value in a group of values. The rank is one plus
    the number of rows preceding the row that are not peer with the row.
    Thus, tie values in the ordering will produce gaps in the sequence.
    The ranking is performed for each window partition.

.. function:: row_number() -> bigint

    Returns a unique, sequential number for each row, starting with one,
    according to the ordering of rows within the window partition.

Value Functions
---------------

Value functions provide an option to specify how null values should be treated when evaluating the
function. Nulls can either be ignored (``IGNORE NULLS``) or respected (``RESPECT NULLS``). By default,
null values are respected. If ``IGNORE NULLS`` is specified, all rows where the value expression is
null are excluded from the calculation. If ``IGNORE NULLS`` is specified and the value expression is
null for all rows, the ``default_value`` is returned, or if it is not specified, ``null`` is returned.

.. function:: first_value(x) -> [same as input]

    Returns the first value of the window.

.. function:: last_value(x) -> [same as input]

    Returns the last value of the window.

.. function:: nth_value(x, offset) -> [same as input]

    Returns the value at the specified offset from beginning the window.
    Offsets start at ``1``. The offset can be any scalar
    expression.  If the offset is null or greater than the number of values in
    the window, null is returned.  It is an error for the offset to be zero or
    negative.

.. function:: lead(x[, offset [, default_value]]) -> [same as input]

    Returns the value at ``offset`` rows after the current row in the window partition.
    Offsets start at ``0``, which is the current row. The
    offset can be any scalar expression. The default ``offset`` is ``1``. If the
    offset is ``null``, ``null`` is returned. If the offset refers to a row that is not
    within the partition, the ``default_value`` is returned, or if it is not specified
    ``null`` is returned.

.. function:: lag(x[, offset [, default_value]]) -> [same as input]

    Returns the value at ``offset`` rows before the current row in the window partition.
    Offsets start at ``0``, which is the current row. The
    offset can be any scalar expression. The default ``offset`` is ``1``. If the
    offset is ``null``, ``null`` is returned. If the offset refers to a row that is not
    within the partition, the ``default_value`` is returned, or if it is not specified
    ``null`` is returned.
