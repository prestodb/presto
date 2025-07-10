=========================
Quantile Digest Functions
=========================

Quantile digest and `T-digest <https://doi.org/10.1016/j.simpa.2020.100049>`_ are two
older algorithms for estimating rank-based metrics. While T-digest generally has `better
performance <https://arxiv.org/abs/1902.04023>`_ and better accuracy at the tails,
quantile digest supports more numeric types (``bigint``, ``double``, ``real``) and
provides a maximum rank error guarantee, which ensures relative uniformity of precision
along the quantiles. Quantile digests are also formally proven to support lossless merges,
while T-digest is not (though it does empirically demonstrate lossless merges).

Velox uses the modern KLL sketch algorithm for the ``approx_percentile`` function, which
provides stronger accuracy guarantees than both quantile digest and T-digest.
The quantile digest functions documented here exist primarily to support
pre-existing workloads that have data stored using the `quantile
digest <http://dx.doi.org/10.1145/347090.347195>`_ format for backward compatibility.

Data Structures
---------------

A quantile digest is a data sketch which stores approximate percentile
information. The Velox type for this data structure is called ``qdigest``,
and it takes a parameter which must be one of ``bigint``, ``double`` or
``real`` which represent the set of numbers that may be ingested by the
``qdigest``. They may be merged without losing precision, and for storage
and retrieval they may be cast to/from ``VARBINARY``.

In the function signatures below, ``T`` represents the parameterized type of the qdigest,
which can be ``bigint``, ``double``, or ``real``.

Functions
---------

.. function:: merge(qdigest<T>) -> qdigest<T>

    Merges all input ``qdigest``\ s into a single ``qdigest``.

.. function:: qdigest_agg(x: T) -> qdigest<T>

    Returns the ``qdigest`` which summarizes the approximate distribution of all input values of ``x``.

.. function:: qdigest_agg(x: T, w: bigint) -> qdigest<T>
   :noindex:

    Returns the ``qdigest`` which summarizes the approximate distribution of all input values of ``x`` using
    the per-item weight ``w``.

.. function:: qdigest_agg(x: T, w: bigint, accuracy: double) -> qdigest<T>
   :noindex:

    Returns the ``qdigest`` which summarizes the approximate distribution of all input values of ``x`` using
    the per-item weight ``w`` and maximum error of ``accuracy``. ``accuracy``
    must be a value greater than zero and less than one, and it must be constant
    for all input rows.

.. function:: value_at_quantile(digest: qdigest<T>, quantile: double) -> T

    Returns the approximate percentile values from the quantile digest ``digest`` given the ``quantile``.
    The ``quantile`` must be between zero and one (inclusive).

.. function:: values_at_quantiles(digest: qdigest<T>, quantiles: array<double>) -> array<T>

    Returns the approximate percentile values as an array from the quantile digest ``digest`` at each of the specified quantiles given in the ``quantiles`` array.
    All quantile values must be between zero and one (inclusive).
