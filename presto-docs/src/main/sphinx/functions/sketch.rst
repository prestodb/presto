================
Sketch Functions
================

Sketches are data structures that can approximately answer particular questions
about a dataset when full accuracy is not required. The benefit of approximate
answers is that they are often faster and more efficient to compute than
functions which result in full accuracy.

Presto provides support for computing some sketches available in the `Apache
DataSketches`_ library. 

Theta Sketches
--------------

Theta sketches enable distinct value counting on datasets and also provide the
ability to perform set operations. For more information on Theta sketches,
please see the Apache Datasketches `Theta sketch documentation`_.

.. function:: sketch_theta(x) -> varbinary

    Computes a `theta sketch`_ from an input dataset. The output from
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

Kll Sketches are an implementation of a quantiles sketch. For more information
about the KLL sketch, see the Apache Datasketches `KLL Sketch documentation`_.


.. function:: sketch_kll[T](x: T) -> kllsketch[T]

    This computes a KLL Sketch. The stored form is the little-endian serialized
    version of the Apache DataSketches KLL Sketch.

.. function:: sketch_kll_with_k[T](x: T, k: int) -> kllsketch[T]

    This computes a KLL Sketch using the supplied value for ``k``. The ``k`` parameter must be in
    the range [8..65535]. It controls the accuracy of the sketch. Smaller ``k`` is less accurate but
    consumes less storage. A larger ``k`` will be more accurate but consume more storage. For more
    information on the ``k`` parameter, refer to the `KLL Sketch documentation`_. The serialized
    form of the sketch returned by this function is the same as the `sketch_kll` function.

.. function:: sketch_kll_quantile[T](sketch: kllsketch[T], rank: double[, inclusivity: boolean]) -> T

    Computes the value in the sketch that occurs at a particular quantile. The
    third argument refers to the inclusivity of the query. This function returns
    values strictly less than the quantile when inclusivity is false or values
    less than or equal to the quantile when inclusivity true. If omitted, the
    default inclusivity is true.

.. function:: sketch_kll_rank[T](sketch: kllsketch[T], quantile: T[, inclusivity: boolean]) -> double

    Computes the quantile that a particular value occurs at in the sketch. The
    third argument refers to the inclusivity of the query. Given that the sketch
    represents a distribution of data ``X``, this function returns quantiles
    representing ``P(T < X)`` when inclusivity is false and ``P(T <= X)`` when
    inclusivity is true. If omitted, the default inclusivity is true.


.. _Apache DataSketches: https://datasketches.apache.org/
.. _theta sketch: https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html
.. _Theta sketch documentation: https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html
.. _KLL Sketch documentation: https://datasketches.apache.org/docs/KLL/KLLSketch.html