===================
Aggregate Functions
===================

Aggregate functions operate on a set of values to compute a single result.

General Aggregate Functions
---------------------------

.. spark:function:: avg(x) -> double|decimal

    Returns the average (arithmetic mean) of all non-null input values.
    When x is of type DECIMAL, the result type is DECIMAL,
    and the intermediate results are varbinarys or (sum, count) pairs represented as row(decimal, bigint).
    For all other input types, the result type is DOUBLE,
    and the intermediate results are (sum, count) pairs represented as row(double, bigint).
    When all inputs are nulls, the intermediate result is row(0, 0),
    and the final result is null.

.. spark:function:: bit_xor(x) -> bigint

    Returns the bitwise XOR of all non-null input values, or null if none.

.. spark:function:: bloom_filter_agg(hash, estimatedNumItems, numBits) -> varbinary

    Creates bloom filter from input hashes and returns it serialized into VARBINARY.
    The caller is expected to apply xxhash64 function to input data before calling bloom_filter_agg.

    For example, 
        bloom_filter_agg(xxhash64(x), 100, 1024)
    In Spark implementation, ``estimatedNumItems`` and ``numBits`` are used to decide the number of hash functions and bloom filter capacity.
    In Velox implementation, ``estimatedNumItems`` is not used.

    ``hash`` cannot be null.
    ``numBits`` specifies max capacity of the bloom filter, which allows to trade accuracy for memory.
    In Spark, the value of ``numBits`` is automatically capped at config value 67,108,864.
    In Velox, the value of ``numBits`` is automatically capped at the value of spark.bloom_filter.max_num_bits configuration property.

    ``hash``, ``estimatedNumItems`` and ``numBits`` must be ``BIGINT``.

.. spark:function:: bloom_filter_agg(hash, estimatedNumItems) -> varbinary

    A version of ``bloom_filter_agg`` that uses ``numBits`` computed as ``estimatedNumItems`` * 8.

    ``hash`` cannot be null.
    ``estimatedNumItems`` provides an estimate of the number of values of ``x`` under the fact of ``hash`` is xxhash64(x).
    Value of ``estimatedNumItems`` is capped at 4,000,000 like to match Spark's implementation.
    But Spark allows for changing the defaults while Velox does not.

.. spark:function:: bloom_filter_agg(hash) -> varbinary
    
    A version of ``bloom_filter_agg`` that use the value of spark.bloom_filter.max_num_bits configuration property as ``numBits``.

    ``hash`` cannot be null.

.. spark:function:: collect_list(x) -> array<[same as x]>

    Returns an array created from the input ``x`` elements. Ignores null
    inputs, and returns an empty array when all inputs are null.

.. spark:function:: first(x) -> x

    Returns the first value of `x`.

.. spark:function:: first_ignore_null(x) -> x

    Returns the first non-null value of `x`.

.. spark:function:: kurtosis(x) -> double

    Returns the Pearson's kurtosis of all input values. When the count of `x` is not empty,
    a non-null output will be generated. When the value of `m2` in the accumulator is 0, a null
    output will be generated.

.. spark:function:: last(x) -> x

    Returns the last value of `x`.

.. spark:function:: last_ignore_null(x) -> x

    Returns the last non-null value of `x`.

.. spark:function:: max_by(x, y) -> [same as x]

    Returns the value of `x` associated with the maximum value of `y`.
    Note: Spark provides a non-strictly comparator which is greater than or equals to.

    Example::

        SELECT max_by(x, y)
        FROM (
            VALUES
                ('a', 10),
                ('b', 50),
                ('c', 50)
        ) AS t(x, y);

    Returns c

.. spark:function:: min_by(x, y) -> [same as x]

    Returns the value of `x` associated with the minimum value of `y`.
    Note: Spark provides a non-strictly comparator which is less than or equals to.

    Example::

        SELECT min_by(x, y)
        FROM (
            VALUES
                ('a', 10),
                ('b', 10),
                ('c', 50)
        ) AS t(x, y);

    Returns b

.. spark:function:: regr_replacement(x) -> double

    Returns the `m2` (the sum of the second central moment) of input values.

.. spark:function:: skewness(x) -> double

    Returns the skewness of all input values. When the count of `x` is greater than or equal to 1,
    a non-null output will be generated. When the value of `m2` in the accumulator is 0, a null
    output will be generated.

.. spark:function:: sum(x) -> bigint|double|real

    Returns the sum of `x`.

    Supported types are TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE and DECIMAL.

    When x is of type DOUBLE, the result type is DOUBLE.
    When x is of type REAL, the result type is REAL.
    When x is of type DECIMAL(p, s), the result type is DECIMAL(p + 10, s), where (p + 10) is capped at 38.

    For all other input types, the result type is BIGINT.

    Note:
    When all input values is NULL, for all input types, the result is NULL.

    For DECIMAL type, when an overflow occurs in the accumulation, it returns NULL. For REAL and DOUBLE type, it
    returns Infinity. For all other input types, when the sum of input values exceeds its limit, it cycles to the
    overflowed value rather than raising an error.

    Example::

        SELECT SUM(x)
        FROM (
            VALUES
                (9223372036854775807L),
                (1L)
        ) AS t(x);

    Returns -9223372036854775808
