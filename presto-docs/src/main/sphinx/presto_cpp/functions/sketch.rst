================
Sketch Functions
================

Sketches are data structures that can approximately answer particular questions
about a dataset when full accuracy is not required. Approximate answers are
often faster and more efficient to compute than functions which result in full
accuracy.

Presto C++ provides support for computing some sketches available in the `Apache
DataSketches`_ library.

Theta Sketches
--------------

Theta sketches enable distinct value counting on datasets and also provide the
ability to perform set operations. For more information on Theta sketches,
please see the Apache DataSketches `Theta sketch documentation`_.

.. function:: sketch_theta(x) -> varbinary

    Computes a theta sketch from an input dataset. The output from
    this function can be used as an input to any of the other ``sketch_theta_*``
    family of functions.

.. function:: sketch_theta_estimate(sketch) -> double

    Returns the estimate of distinct values from the input sketch.

.. function:: sketch_theta_summary(sketch) -> row(estimate double, theta double, upper_bound_std double, lower_bound_std double, retained_entries int)

    Returns a summary of the input sketch which includes the distinct values
    estimate alongside other useful information such as the sketch theta
    parameter, current error bounds corresponding to 1 standard deviation, and
    the number of retained entries in the sketch.

KLL Sketches
------------

KLL sketches enable approximate quantile estimation and rank queries on datasets.
For more information on KLL sketches, please see the Apache DataSketches
`KLL sketch documentation`_.

.. function:: sketch_kll(x) -> varbinary

    Computes a KLL sketch from an input dataset with default k=200. The output
    from this function can be used as an input to any of the other ``sketch_kll_*``
    family of functions. Supported types: ``bigint``, ``double``, ``varchar``,
    ``boolean``.

.. function:: sketch_kll_with_k(x, k) -> varbinary

    Computes a KLL sketch from an input dataset with a custom k parameter
    (8 ≤ k ≤ 65535). Higher k values provide better accuracy but use more memory.
    Supported types: ``bigint``, ``double``, ``varchar``, ``boolean``.

.. function:: sketch_kll_rank(sketch, value) -> double

    Returns the approximate rank (percentile) of the given value in the sketch.
    The rank is a value between 0.0 and 1.0, where 0.0 represents the minimum
    value and 1.0 represents the maximum value. By default, uses inclusive mode
    (values less than or equal to the given value). Supported types: ``bigint``,
    ``double``, ``varchar``, ``boolean``.

    Example::

        SELECT sketch_kll_rank(sketch_kll(price), 50.0) FROM orders;
        -- Returns the fraction of prices that are ≤ 50.0

.. function:: sketch_kll_rank(sketch, value, inclusive) -> double

    Returns the approximate rank of the given value in the sketch with explicit
    boundary mode control. When ``inclusive`` is ``true``, counts values less than
    or equal to the given value. When ``false``, counts only values strictly less
    than the given value.

    Example::

        SELECT sketch_kll_rank(sketch_kll(price), 50.0, false) FROM orders;
        -- Returns the fraction of prices that are < 50.0 (exclusive)

.. _Apache DataSketches: https://datasketches.apache.org/
.. _Theta sketch documentation: https://datasketches.apache.org/docs/Theta/ThetaSketches.html#theta-sketch-framework
.. _KLL sketch documentation: https://datasketches.apache.org/docs/KLL/KLLSketch.html
