================
Window Functions
================

Window functions perform calculations across rows of the query result.
They run after the ``HAVING`` clause but before the ``ORDER BY`` clause.
Invoking a window function requires special syntax using the ``OVER``
clause to specify the window. A window has three components:

* The partition specification, which separates the input rows into different
  partitions. This is analogous to how the ``GROUP BY`` clause separates rows
  into different groups for aggregate functions.
* The ordering specification, which determines the order in which input rows
  will be processed by the window function.
* The window frame, which specifies a sliding window of rows to be processed
  by the function for a given row.

For example, the following query ranks orders for each clerk by price::

    SELECT orderid, clerk, totalprice,
           RANK() OVER (PARTITION BY clerk
                        ORDER BY totalprice DESC) AS rnk
    FROM orders
    ORDER BY clerk, rnk

Ranking Functions
-----------------

.. function:: cume_dist() -> bigint

    Returns the cumulative distribution of a value in a group of values.
    The result is the number of rows preceding or peer with the row in the
    window ordering of the window partition divided by the total number of
    rows in the window partition. Thus, any tie values in the ordering will
    evaluate to the same distribution value.

.. function:: dense_rank() -> bigint

    Returns the rank of a value in a group of values. This is similar to
    :func:`rank`, except that tie values do not produce gaps in the sequence.

.. function:: percent_rank() -> bigint

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

.. warning::
    These functions do not respect the window frame (default or specified) and
    instead operate on the entire partition.  This is equivalent to ``RANGE
    BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING``.  In a future
    release, these functions will be changed to respect the window frame and to
    use the standard default window frame ``RANGE BETWEEN UNBOUNDED PRECEDING
    AND CURRENT ROW``.


.. function:: first_value(x) -> [same as input]

    Returns the first value of the window (see warning above).

.. function:: last_value(x) -> [same as input]

    Returns the last value of the window (see warning above).

.. function:: nth_value(x, offset) -> [same as input]

    Returns the value at the specified offset from beginning the window  (see
    warning above).  Offsets start at ``1``. The offset can be any scalar
    expression.  If the offset is null or greater than the number of values in
    the window, null is returned.  It is an error for the offset to be zero or
    negative.
