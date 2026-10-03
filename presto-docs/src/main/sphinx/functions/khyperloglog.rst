======================
KHyperLogLog Functions
======================

Presto implements the `KHyperLogLog <https://research.google/pubs/pub47664/>`_
algorithm and data structure. ``KHyperLogLog`` data structure can be created
through :func:`!khyperloglog_agg`.


Data Structures
---------------

KHyperLogLog is a data sketch that compactly represents the association of two
columns. It is implemented in Presto as a two-level data structure composed of
a MinHash structure whose entries map to ``HyperLogLog``.

Serialization
-------------

KHyperLogLog sketches can be cast to and from ``varbinary``. This allows them to
be stored for later use.

Functions
---------

.. function:: khyperloglog_agg(x, y) -> KHyperLogLog

    Returns the ``KHyperLogLog`` sketch that represents the relationship between
    columns ``x`` and ``y``. The MinHash structure summarizes ``x`` and the HyperLogLog
    sketches represent ``y`` values linked to ``x`` values.

.. function:: khyperloglog_agg_java_compat(x, y) -> KHyperLogLog

    Alias of :func:`khyperloglog_agg`, provided for cross-engine portability.

    The native (C++) engine's ``khyperloglog_agg`` hashes its inputs differently
    from the Java implementation, so sketches built by the two engines are not
    byte compatible and cannot be merged with each other. The native engine
    registers ``khyperloglog_agg_java_compat`` as a separate aggregate that
    reproduces the Java hashing exactly, leaving its own ``khyperloglog_agg``
    unchanged so existing sketches stay reproducible.

    Use this name when a sketch must be built by either engine and later merged
    or compared. On Java it behaves identically to :func:`khyperloglog_agg`.

.. function:: cardinality(khll) -> bigint
    :noindex:

    This calculates the cardinality of the MinHash sketch, that is, ``x``'s cardinality.

.. function:: intersection_cardinality(khll1, khll2) ->  bigint

    Returns the set intersection cardinality of the data represented by the MinHash
    structures of ``khll1`` and ``khll2``.

.. function:: jaccard_index(khll1, khll2) ->  double

    Returns the Jaccard index of the data represented by the MinHash structures of
    ``khll1`` and ``khll2``.

.. function:: uniqueness_distribution(khll) ->  map<bigint,double>

    For a certain value ``x'``, uniqueness is understood as how many ``y'`` values are
    associated with it in the source dataset. This is obtained with the cardinality
    of the HyperLogLog that is mapped from the MinHash bucket that corresponds to
    ``x'``. This function returns a histogram that represents the uniqueness
    distribution, the X-axis being the ``uniqueness`` and the Y-axis being the relative
    frequency of ``x`` values. The histogram size defaults to the current size of the
    MinHash structure in the ``KHyperLogLog`` sketch.

.. function:: uniqueness_distribution(khll, histogramSize) ->  map<bigint,double>

    Returns the uniqueness histogram with the given number of buckets, ``histogramSize``.
    All ``uniqueness`` values greater than ``histogramSize`` are accumulated
    in the last bucket.

.. function:: reidentification_potential(khll, threshold) ->  double

    The reidentification potential is the ratio of ``x`` values that have a
    ``uniqueness`` under the given ``threshold``.

.. function:: merge(khll) -> KHyperLogLog
    :noindex:

    Returns the ``KHyperLogLog`` of the aggregate union of the individual ``KHyperLogLog``
    structures.

.. function:: merge_khll(array[khll]) -> KHyperLogLog

    Returns the ``KHyperLogLog`` of the union of an array of KHyperLogLog structures.
