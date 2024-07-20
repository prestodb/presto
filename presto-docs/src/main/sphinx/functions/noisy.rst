=========================
Noisy Aggregate Functions
=========================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

Noisy aggregate functions are functions that provide random, noisy
approximations of common aggregations like :func:`!sum`, :func:`!count`, and
:func:`!approx_distinct` as well as sketches like :func:`!approx_set`. By
injecting random noise into results, noisy aggregation functions make it more
difficult to determine or confirm the exact data that was aggregated.

While many of these functions resemble
`differential privacy <https://en.wikipedia.org/wiki/Differential_privacy>`_
mechanisms, neither the values returned by these functions nor the query results
that incorporate these functions are differentially private in general.
See `Limitations`_ below for more details. Users who wish to support a strong
privacy guarantee should discuss with a suitable technical expert first.

Counts, Sums, and Averages
--------------------------

.. function:: noisy_count_gaussian(col, noise_scale[, random_seed]) -> bigint

    Counts the non-``NULL`` values in ``col`` and then adds a normally distributed
    random double value with 0 mean and standard deviation of ``noise_scale`` to the true count.
    The noisy count is post-processed to be non-negative and rounded to bigint.

    If provided, ``random_seed`` is used to seed the random number generator. Otherwise,
    noise is drawn from a secure random. ::

        SELECT noisy_count_gaussian(orderkey, 20.0) FROM tpch.tiny.lineitem; -- 60179 (1 row)
        SELECT noisy_count_gaussian(orderkey, 20.0) FROM tpch.tiny.lineitem WHERE false; -- NULL (1 row)

    .. note::

        Unlike :func:`!count`, this function returns ``NULL`` when the (true) count of ``col`` is 0.

    Distinct counting can be performed using ``noisy_count_gaussian(DISTINCT col, ...)``, or with
    :func:`!noisy_approx_distinct_sfm`. Generally speaking, :func:`!noisy_count_gaussian`
    returns more accurate results but at a larger computational cost.

.. function:: noisy_count_if_gaussian(col, noise_scale[, random_seed]) -> bigint

    Counts the ``TRUE`` values in ``col`` and then adds a normally distributed random double
    value with 0 mean and standard deviation of ``noise_scale`` to the true count.
    The noisy count is post-processed to be non-negative and rounded to bigint.

    If provided, ``random_seed`` is used to seed the random number generator. Otherwise,
    noise is drawn from a secure random. ::

        SELECT noisy_count_if_gaussian(orderkey > 10000, 20.0) FROM tpch.tiny.lineitem; -- 50180 (1 row)
        SELECT noisy_count_if_gaussian(orderkey > 10000, 20.0) FROM tpch.tiny.lineitem WHERE false; -- NULL (1 row)

    .. note::

        Unlike :func:`!count_if`, this function returns ``NULL`` when the (true) count is 0.

.. function:: noisy_sum_gaussian(col, noise_scale, lower, upper[, random_seed]) -> double

    Calculates the sum over the input values in ``col`` and then adds a normally distributed
    random double value with 0 mean and standard deviation of noise_scale. Each value is clipped to the range
    of [``lower``, ``upper``] before adding to the sum.

    If provided, ``random_seed`` is used to seed the random number generator. Otherwise,
    noise is drawn from a secure random.

.. function:: noisy_sum_gaussian(col, noise_scale[, random_seed]) -> double
    :noindex:

    Calculates the sum over the input values in ``col`` and then adds a normally
    distributed random double value with 0 mean and standard deviation of ``noise_scale``.

    If provided, ``random_seed`` is used to seed the random number generator. Otherwise,
    noise is drawn from a secure random.

.. function:: noisy_avg_gaussian(col, noise_scale, lower, upper[, random_seed]) -> double

    Calculates the average (arithmetic mean) of all the input values in ``col`` and then
    adds a normally distributed random double value with 0 mean and standard deviation of ``noise_scale``.
    Each value is clipped to the range of [``lower``, ``upper``] before averaging.

    If provided, ``random_seed`` is used to seed the random number generator. Otherwise,
    noise is drawn from a secure random.

.. function:: noisy_avg_gaussian(col, noise_scale[, random_seed]) -> double
    :noindex:

    Calculates the average (arithmetic mean) of all the input values in ``col`` and then adds
    a normally distributed random double value with 0 mean and standard deviation of ``noise_scale``.

    If provided, ``random_seed`` is used to seed the random number generator. Otherwise,
    noise is drawn from a secure random.


Approximate Distinct Counting/Sketching
---------------------------------------

Noisy approximate distinct counting and sketching (analogous to the deterministic :doc:`hyperloglog`)
is supported via the Sketch-Flip-Merge (SFM) data sketch [Hehir2023]_.

.. function:: noisy_approx_set_sfm(col, epsilon[, buckets[, precision]]) -> SfmSketch

    Returns an SFM sketch of the input values in ``col``. This is analogous to the
    :func:`!approx_set` function, which returns a (deterministic) HyperLogLog sketch.

    - ``col`` supports many types, similar to ``HyperLogLog``.
    - ``epsilon`` (double) is a positive number that controls the level of noise in
      the sketch, as described in [Hehir2023]_. Smaller values of epsilon correspond
      to noisier sketches.
    - ``buckets`` (int) defaults to 4096.
    - ``precision`` (int) defaults to 24.


    .. note::

        Unlike :func:`!approx_set`, this function returns ``NULL`` when ``col`` is empty.
        If this behavior is undesirable, use :func:`!coalesce` with :func:`!noisy_empty_approx_set_sfm`.

.. function:: noisy_approx_set_sfm_from_index_and_zeros(col_index, col_zeros, epsilon, buckets[, precision]) -> SfmSketch

    Returns an SFM sketch of the input values in ``col_index`` and ``col_zeros``.

    This is similar to :func:`!noisy_approx_set_sfm` except that function calculates a ``Murmur3Hash128.hash64()`` of ``col``,
    and calculates the SFM PCSA bucket index and number of trailing zeros as described in
    [FlajoletMartin1985]_. In this function, the caller must explicitly calculate the hash bucket index
    and zeros themselves and pass them as arguments ``col_index`` and ``col_zeros``.

    - ``col_index`` (bigint) must be in the range ``0..buckets-1``.
    - ``col_zeros`` (bigint) must be in the range ``0..64``. If it exceeds ``precision``, it
      is cropped to ``precision-1``.
    - ``epsilon`` (double) is a positive number that controls the level of noise in
      the sketch, as described in [Hehir2023]_. Smaller values of epsilon correspond
      to noisier sketches.
    - ``buckets`` (int) is the number of buckets in the SFM PCSA sketch as described in [Hehir2023]_.
    - ``precision`` (int) defaults to 24.

    .. note::

        Like  :func:`!noisy_approx_set_sfm`, this function returns ``NULL`` when ``col_index``
        or ``col_zeros`` is ``NULL``.
        If this behavior is undesirable, use :func:`!coalesce` with :func:`!noisy_empty_approx_set_sfm`.

.. function:: noisy_approx_distinct_sfm(col, epsilon[, buckets[, precision]]) -> bigint

    Equivalent to ``cardinality(noisy_approx_set_sfm(col, epsilon, buckets, precision))``,
    this returns the approximate cardinality (distinct count) of the column ``col``.
    This is analogous to the (deterministic) :func:`!approx_distinct` function.

    .. note::

        Unlike :func:`!approx_distinct`, this function returns ``NULL`` when ``col`` is empty.

.. function:: noisy_empty_approx_set_sfm(epsilon[, buckets[, precision]]) -> SfmSketch

    Returns an SFM sketch with no items in it. This is analogous to the :func:`!empty_approx_set`
    function, which returns an empty (deterministic) HyperLogLog sketch.

    - ``epsilon`` (double) is a positive number that controls the level of noise in the sketch,
      as described in [Hehir2023]_. Smaller values of epsilon correspond to noisier sketches.
    - ``buckets`` (int) defaults to 4096.
    - ``precision`` (int) defaults to 24.

.. function:: cardinality(SfmSketch) -> bigint

    Returns the estimated cardinality (distinct count) of an ``SfmSketch`` object.

.. function:: merge(SfmSketch) -> SfmSketch

    An aggregator function that returns a merged ``SfmSketch`` of the set union of
    individual ``SfmSketch`` objects, similar to ``merge(HyperLogLog)``. ::

        SELECT year, cardinality(merge(sketch)) AS annual_distinct_count
        FROM monthly_sketches
        GROUP BY 1

.. function:: merge_sfm(ARRAY[SfmSketch, ...]) -> SfmSketch

    A scalar function that returns a merged ``SfmSketch`` of the set union of an array
    of ``SfmSketch`` objects, similar to :func:`!merge_hll`. ::

        SELECT cardinality(merge_sfm(ARRAY[
            noisy_approx_set_sfm(col_1, 5.0),
            noisy_approx_set_sfm(col_2, 5.0),
            noisy_approx_set_sfm(col_3, 5.0)
        ])) AS distinct_count_over_3_cols
        FROM my_table


Limitations
-----------

While these functions resemble differential privacy mechanisms, the values returned
by these functions are not differentially private in general. There are several
important limitations to keep in mind if using these functions for
privacy-preserving purposes, including:

- All noisy aggregate functions return ``NULL`` when aggregating empty sets.
  This means a ``NULL`` return value noiselessly indicates the absence of data.
- ``GROUP BY`` clauses used in combination with noisy aggregation functions
  reveal non-noisy information: the presence or absence of a group noiselessly
  indicates the presence or absence of data. See, e.g., [Wilkins2024]_.
- Functions relying on floating-point noise may be susceptible to inference
  attacks such as those identified in [Mironov2012]_ and [Casacuberta2022]_.

---------------------------

.. [Casacuberta2022] Casacuberta, S., Shoemate, M., Vadhan, S., & Wagaman, C.
    (2022). `Widespread Underestimation of Sensitivity in Differentially Private
    Libraries and How to Fix It. <https://arxiv.org/pdf/2207.10635>`_ In *Proceedings
    of the 2022 ACM SIGSAC Conference on Computer and Communications Security* (pp. 471-484).

.. [Hehir2023] Hehir, J., Ting, D., & Cormode, G. (2023). `Sketch-Flip-Merge:
    Mergeable Sketches for Private Distinct Counting.
    <https://proceedings.mlr.press/v202/hehir23a/hehir23a.pdf>`_ In *Proceedings of
    the 40th International Conference on Machine Learning* (Vol. 202).

.. [Mironov2012] Mironov, I. (2012). `On significance of the least significant bits
    for differential privacy. <https://www.microsoft.com/en-us/research/wp-content/uploads/2012/10/lsbs.pdf>`_
    In *Proceedings of the 2012 ACM Conference on Computer and Communications Security* (pp. 650-661).

.. [Wilkins2024] Wilkins, A., Kifer, D., Zhang, D., & Karrer, B. (2024). `Exact
    Privacy Analysis of the Gaussian Sparse Histogram Mechanism.
    <https://journalprivacyconfidentiality.org/index.php/jpc/article/view/823/755>`_
    *Journal of Privacy and Confidentiality*, 14 (1).

.. [FlajoletMartin1985] Flajolet, P, Martin, G. N. (1985). `Probabilistic Counting Algorithms for Data Base Applications.
   <https://algo.inria.fr/flajolet/Publications/src/FlMa85.pdf>`_
   In *Journal of Computer and System Sciences*, 31:182–209, 1985 

