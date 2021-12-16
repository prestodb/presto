=========================
Quantile Digest Functions
=========================

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

Presto implements the ``approx_percentile``  function with the quantile digest
data structure.  The underlying data structure, :ref:`qdigest <qdigest_type>`,
is exposed as a data type in Presto, and can be created, queried and stored
separately from ``approx_percentile``.

Data Structures
---------------

A quantile digest is a data sketch which stores approximate percentile
information.  The presto type for this data structure is called ``qdigest``,
and it takes a parameter which must be one of ``bigint``, ``double`` or
``real`` which represent the set of numbers that may be ingested by the
``qdigest``.  They may be merged without losing precision, and for storage
and retrieval they may be cast to/from ``VARBINARY``.

Functions
---------

.. function:: merge(qdigest) -> qdigest
    :noindex:

    Merges all input ``qdigest``\ s into a single ``qdigest``.

.. function:: value_at_quantile(qdigest(T), quantile) -> T

    Returns the approximate percentile values from the quantile digest given
    the number ``quantile`` between 0 and 1.

.. function:: quantile_at_value(qdigest(T), T) -> quantile

    Returns the approximate ``quantile`` number between 0 and 1 from the
    quantile digest given an input value. Null is returned if the quantile digest
    is empty or the input value is outside of the range of the quantile digest.

.. function:: scale_qdigest(qdigest(T), scale_factor) -> qdigest(T)

    Returns a ``qdigest`` whose distribution has been scaled by a factor
    specified by ``scale_factor``.

.. function:: values_at_quantiles(qdigest(T), quantiles) -> T

    Returns the approximate percentile values as an array given the input
    quantile digest and array of values between 0 and 1 which
    represent the quantiles to return.

.. function:: qdigest_agg(x) -> qdigest<[same as x]>

    Returns the ``qdigest`` which is composed of  all input values of ``x``.

.. function:: qdigest_agg(x, w) -> qdigest<[same as x]>

    Returns the ``qdigest`` which is composed of  all input values of ``x`` using
    the per-item weight ``w``.

.. function:: qdigest_agg(x, w, accuracy) -> qdigest<[same as x]>

    Returns the ``qdigest`` which is composed of  all input values of ``x`` using
    the per-item weight ``w`` and maximum error of ``accuracy``. ``accuracy``
    must be a value greater than zero and less than one, and it must be constant
    for all input rows.
