===========================
Sketch Functions
===========================

Sketches are data structures that can approximately answer particular questions
about a dataset when full accuracy is not required. The benefit of approximate
answers is that they are often faster and more efficient to compute than
functions which result in full accuracy.

Presto provides support for computing some sketches available in the `Apache
DataSketches`_ library. 

.. function:: sketch_theta(data) -> varbinary

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


.. _Apache DataSketches: https://datasketches.apache.org/
.. _theta sketch: https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html