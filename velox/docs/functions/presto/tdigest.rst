==================
T-Digest Functions
==================

T-digest and `quantile digest <http://dx.doi.org/10.1145/347090.347195>`_ are two
older algorithms for estimating rank-based metrics. T-digest generally has `better
performance <https://arxiv.org/abs/1902.04023>`_ than quantile digest and better accuracy
at the tails (often dramatically better), but may have worse accuracy at the median
depending on the compression factor used. In comparison, quantile digest supports more
numeric types and provides a maximum rank error guarantee, which ensures relative uniformity
of precision along the quantiles. Quantile digests are also formally proven to support
lossless merges, while T-digest is not (though it does empirically demonstrate lossless merges).

T-digest was developed by Ted Dunning and is more restrictive in its type support,
accepting only ``double`` type parameters. This contrasts with quantile digest, which
supports a broader range of numeric types including ``bigint``, ``double``, and ``real``,
making quantile digest more versatile for different data types.

Velox uses the modern KLL sketch algorithm for the ``approx_percentile`` function, which
provides stronger accuracy guarantees than both T-digest and quantile digest.
The T-digest functions documented here exist primarily to support
pre-existing workloads that have data stored using the `T-digest
<https://doi.org/10.1016/j.simpa.2020.100049>`_ format for backward compatibility.

Data Structures
---------------

A T-digest is a data sketch which stores approximate percentile information.
The Velox type for this data structure is called ``tdigest``,
and it accepts a parameter of type ``double`` which represents the set of
numbers to be ingested by the ``tdigest``.

T-digests may be merged without losing precision, and for storage and retrieval
they may be cast to/from ``VARBINARY``.

Functions
---------

.. function:: construct_tdigest(means: array<double>, counts: array<integer>, compression: double, min: double, max: double, sum: double, count: bigint) -> tdigest<double>

    Constructs a T-digest from the given parameters:

    * ``means`` - array of centroid means
    * ``counts`` - array of centroid counts (weights)
    * ``compression`` - compression factor
    * ``min`` - minimum value
    * ``max`` - maximum value
    * ``sum`` - sum of all values
    * ``count`` - total count of values

.. function:: destructure_tdigest(digest: tdigest<double>) -> row(means array<double>, counts array<integer>, compression double, min double, max double, sum double, count bigint)

    Destructures a T-digest into its component parts, returning a row containing:

    * ``means`` - array of centroid means
    * ``counts`` - array of centroid counts
    * ``compression`` - compression factor
    * ``min`` - minimum value
    * ``max`` - maximum value
    * ``sum`` - sum of all values
    * ``count`` - total count of values

.. function:: merge(tdigest<double>) -> tdigest<double>

    Merges all input ``tdigest``\ s into a single ``tdigest``.

.. function:: merge_tdigest(digests: array<tdigest<double>>) -> tdigest<double>

    Merges an array of T-digests into a single T-digest.

.. function:: quantile_at_value(digest: tdigest<double>, value: double) -> double

    Returns the approximate quantile (percentile) of the given ``value`` based on the T-digest ``digest``.
    The result will be between zero and one (inclusive).

.. function:: quantiles_at_values(digest: tdigest<double>, values: array<double>) -> array<double>

    Returns the approximate quantiles (percentiles) as an array for each of the given ``values`` based on the T-digest ``digest``.
    All results will be between zero and one (inclusive).

.. function:: scale_tdigest(digest: tdigest<double>, scale: double) -> tdigest<double>

    Scales the T-digest ``digest`` by the given ``scale`` factor.
    This multiplies all the centroid values in the T-digest by the scale factor.

.. function:: tdigest_agg(x: double) -> tdigest<double>

    Returns the ``tdigest`` which summarizes the approximate distribution of all input values of ``x``.
    The default compression factor is ``100``.

.. function:: tdigest_agg(x: double, w: double) -> tdigest<double>
   :noindex:

    Returns the ``tdigest`` which summarizes the approximate distribution of all input values of ``x`` using per-item weight ``w``.
    The default compression factor is ``100``.

.. function:: tdigest_agg(x: double, w: double, compression: double) -> tdigest<double>
   :noindex:

    Returns the ``tdigest`` which summarizes the approximate distribution of all input values of ``x`` using per-item weight ``w`` and the specified compression factor.
    ``compression`` must be a positive constant for all input rows. The default is ``100``, maximum is ``1000``, and values lower than ``10`` are rounded to ``10``. Higher compression means more accuracy at the cost of more memory.

.. function:: trimmed_mean(digest: tdigest<double>, low_quantile: double, high_quantile: double) -> double

    Returns the mean of values between ``low_quantile`` and ``high_quantile`` (inclusive) from the T-digest ``digest``.
    Both quantile values must be between zero and one (inclusive), and ``low_quantile`` must be less than or equal to ``high_quantile``.

.. function:: value_at_quantile(digest: tdigest<double>, quantile: double) -> double

    Returns the approximate percentile value from the T-digest ``digest`` at the given ``quantile``.
    The ``quantile`` must be between zero and one (inclusive).

.. function:: values_at_quantiles(digest: tdigest<double>, quantiles: array<double>) -> array<double>

    Returns the approximate percentile values as an array from the T-digest ``digest`` at each of the specified quantiles given in the ``quantiles`` array.
    All quantile values must be between zero and one (inclusive).
