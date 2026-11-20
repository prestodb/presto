==================
Gaussian functions
==================

A `multivariate normal distribution
<https://wikipedia.org/wiki/Multivariate_normal_distribution>`_ summarises where a set of
points lies and how it is spread. Presto represents one with the :ref:`gaussian_type`
type, which holds the sum of the weights of the points, their mean, and their covariance.

A ``Gaussian`` occupies a fixed amount of space however many points it stands for -- 50
bytes for a two-dimensional distribution -- so it can be stored in a table and queried
later.

Weights are frequency weights: a weight of 3 means the point was observed three times.
The sum of those weights is therefore the number of observations the distribution stands
for, and is what ``weight_sum`` means throughout these functions.

Declaring a distribution
------------------------

.. function:: gaussian(mean, covariance) -> Gaussian

    Returns the distribution with the given mean vector and covariance matrix.

    ``mean`` is an ``array(double)`` whose length is the dimensionality. ``covariance`` is
    a square ``array(array(double))`` of that size, and must be symmetric and positive
    definite -- a matrix describing no extent in some direction is not a distribution, and
    is rejected here rather than producing an unusable value later.

    The weight sum is 1, so the distribution stands for a single observation. That is not
    enough to merge with, because merging needs to know how many observations each side
    represents::

        SELECT gaussian(ARRAY[10.0, 20.0], ARRAY[ARRAY[4.0, 0.0], ARRAY[0.0, 1.0]])

.. function:: gaussian(mean, covariance, weight_sum) -> Gaussian

    As above, for a distribution standing for ``weight_sum`` observations. Use this form
    when the result needs to be merged with others.

Serialization
-------------

A fitted distribution is stored as ``varbinary`` and cast back when it is read. The cast
is free in both directions::

    CREATE TABLE cluster_fits (
      fit_date date,
      cluster_id bigint,
      fit varbinary
    );

    SELECT CAST(fit AS Gaussian) FROM cluster_fits

A cast from ``varbinary`` is unchecked: a value that was not produced by this type is
rejected when a function first reads it, rather than at the cast.
