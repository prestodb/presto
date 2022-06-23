==================
T-Digest Functions
==================

Presto implements two algorithms for estimating rank-based metrics, `quantile
digest <http://dx.doi.org/10.1145/347090.347195>`_ and `T-digest 
<https://doi.org/10.1016/j.simpa.2020.100049>`_.  T-digest has `better
performance <https://arxiv.org/abs/1902.04023>`_ in general while the Presto
implementation of quantile digests supports more numeric types. T-digest has
better accuracy at the tails, often dramatically better, but may have worse
accuracy at the median, depending on the compression factor used. In
comparison, quantile digests supports a maximum rank error, which guarantees
relative uniformity of precision along the quantiles.  Quantile digests are
also formally proven to support lossless merges, while T-digest is not (but
does empirically demonstrate lossless merges).

T-digest was developed by Ted Dunning.

Data Structures
---------------

A T-digest is a data sketch which stores approximate percentile information.
The Presto type for this data structure is called :ref:`tdigest <tdigest_type>`,
and it accepts a parameter of type ``double`` which represents the set of
numbers to be ingested by the ``tdigest``.  Other numeric types may be added
in a future release.

T-digests may be merged without losing precision, and for storage and retrieval
they may be cast to/from ``VARBINARY``.

Functions
---------

.. function:: merge(tdigest<double>) -> tdigest<double>
    :noindex:

    Merges all input ``tdigest``\ s into a single ``tdigest``.

.. function:: value_at_quantile(tdigest<double>, quantile) -> double

    Returns the approximate percentile values from the T-digest given the
    number ``quantile`` between 0 and 1.

.. function:: quantile_at_value(tdigest<double>, value) -> double

    Returns the approximate quantile number between 0 and 1 from the T-digest
    given an input ``value``. Null is returned if the T-digest is empty or the
    input value is outside of the range of the digest.

.. function:: scale_tdigest(tdigest<double>, scale_factor) -> tdigest<double>

    Returns a ``tdigest`` whose distribution has been scaled by a factor
    specified by ``scale_factor``.

.. function:: values_at_quantiles(tdigest<double>, quantiles) -> array<double>

    Returns the approximate percentile values as an array given the input
    T-digest and array of values between 0 and 1 which represent the quantiles
    to return.

.. function:: tdigest_agg(x) -> tdigest<double>

    Returns the ``tdigest`` which is composed of  all input values of ``x``.

.. function:: tdigest_agg(x, w) -> tdigest<double>

    Returns the ``tdigest`` which is composed of  all input values of ``x`` using
    the per-item weight ``w``.

.. function:: tdigest_agg(x, w, compression) -> tdigest<double>

    Returns the ``tdigest`` which is composed of  all input values of ``x`` using
    the per-item weight ``w`` and compression factor ``compression``. ``compression``
    must be a value greater than zero, and it must be constant for all input rows.

    Compression factor of 500 is a good starting point that typically yields good
    accuracy and performance.

.. function:: destructure_tdigest(tdigest<double>) -> row<centroid_means array<double>, centroid_weights array<integer>, compression double, min double, max double, sum double, count bigint>

    Returns a row that represents a ``tdigest`` data structure in the form of
    its component parts. These include arrays of the centroid means and weights,
    the compression factor, and the maximum, minimum, sum and count of the
    values in the digest.
