=====================
HyperLogLog Functions
=====================

Velox implements the Presto's :func:`approx_distinct` function using the
`HyperLogLog <https://en.wikipedia.org/wiki/HyperLogLog>`_ data structure.

Data Structures
---------------

Like Presto, Velox implements HyperLogLog data sketches as a set of 32-bit
buckets which store a *maximum hash*. They can be stored sparsely (as a map
from bucket ID to bucket), or densely (as a contiguous memory block). The
HyperLogLog data structure starts as the sparse representation, switching to
dense when it is more efficient.

Serialization
-------------

Data sketches can be serialized to and deserialized from ``varbinary``. This
allows them to be stored for later use.  Combined with the ability to merge
multiple sketches, this allows one to calculate :func:`approx_distinct` of the
elements of a partition of a query, then for the entirety of a query with very
little cost.

For example, calculating the ``HyperLogLog`` for daily unique users will allow
weekly or monthly unique users to be calculated incrementally by combining the
dailies. This is similar to computing weekly revenue by summing daily revenue.

Serialization format is compatible (actually, identical) to Presto's.

Functions
---------

.. function:: approx_set(x) -> HyperLogLog

    Returns the ``HyperLogLog`` sketch of the input data set of ``x``.
    The value of the maximum standard error is defaulted to ``0.01625``.
    This data sketch underlies :func:`approx_distinct` and can be stored and
    used later by calling ``cardinality()``.

.. function:: approx_set(x, e) -> HyperLogLog
   :noindex:

    Returns the ``HyperLogLog`` sketch of the input data set of ``x``, with
    a maximum standard error of ``e``. The current implementation of this
    function requires that ``e`` be in the range of ``[0.0040625, 0.26000]``.
    This data sketch underlies :func:`approx_distinct` and can be stored and
    used later by calling ``cardinality()``.

.. function:: cardinality(hll) -> bigint
    :noindex:

    This will perform :func:`approx_distinct` on the data summarized by the
    ``hll`` HyperLogLog data sketch.

.. function:: empty_approx_set() -> HyperLogLog

    Returns an empty ``HyperLogLog``.
    The value of the maximum standard error is defaulted to ``0.01625``.

.. function:: empty_approx_set(e) -> HyperLogLog
   :noindex:

    Returns an empty ``HyperLogLog`` with a maximum standard error of ``e``.
    The current implementation of this function requires that ``e`` be in
    the range of ``[0.0040625, 0.26000]``.

.. function:: merge(HyperLogLog) -> HyperLogLog

    Returns the ``HyperLogLog`` of the aggregate union of the individual ``hll``
    HyperLogLog structures.
