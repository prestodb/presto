=========================
Quantile Digest Functions
=========================

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
