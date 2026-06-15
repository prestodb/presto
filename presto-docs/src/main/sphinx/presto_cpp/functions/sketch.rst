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

.. function:: sketch_kll[T](x: T) -> kllsketch[T]

    Computes a KLL sketch from an input dataset with default k=200. The output
    from this function can be used as an input to any of the other
    ``sketch_kll_*`` family of functions.

.. function:: sketch_kll_with_k[T](x: T, k: bigint) -> kllsketch[T]

    Computes a KLL sketch from an input dataset with a custom ``k`` parameter.
    The ``k`` parameter must be in the range [8..65535]. It controls the accuracy
    of the sketch — smaller ``k`` is less accurate but consumes less storage.
    For more information on ``k``, refer to the `KLL sketch documentation`_.

.. function:: sketch_kll_rank[T](sketch: kllsketch[T], value: T[, inclusive: boolean]) -> double

    Returns the approximate rank of ``value`` in the sketch — the fraction of
    values in the sketch that are less than or equal to ``value``. When
    ``inclusive`` is ``false``, counts only values strictly less than ``value``.
    If omitted, the default is ``true``.

.. function:: sketch_kll_quantile[T](sketch: kllsketch[T], rank: double[, inclusive: boolean]) -> T

    Returns the approximate value at the given rank (percentile) in the sketch.
    The ``rank`` must be between 0.0 and 1.0. When ``inclusive`` is ``false``,
    uses exclusive boundaries. If omitted, the default is ``true``.

.. _Apache DataSketches: https://datasketches.apache.org/
.. _Theta sketch documentation: https://datasketches.apache.org/docs/Theta/ThetaSketches.html#theta-sketch-framework
.. _KLL sketch documentation: https://datasketches.apache.org/docs/KLL/KLLSketch.html
