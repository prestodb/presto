====================
Set Digest functions
====================

MinHash, or the min-wise independent permutations locality-sensitive hashing scheme,
is a technique used in computer science to quickly estimate how similar two sets are.
MinHash serves as a probabilistic data structure that estimates the Jaccard similarity
coefficient - the measure of the overlap between two sets as a percentage of the total unique elements in both sets.
Presto offers several functions that deal with the
`MinHash <https://wikipedia.org/wiki/MinHash>`_ technique.

MinHash is used to quickly estimate the
`Jaccard similarity coefficient <https://wikipedia.org/wiki/Jaccard_index>`_
between two sets.
It is commonly used in data mining to detect near-duplicate web pages at scale.
By using this information, the search engines efficiently avoid showing
within the search results two pages that are nearly identical.

Data structures
---------------

Presto implements Set Digest data sketches by encapsulating the following components:

- `HyperLogLog <https://wikipedia.org/wiki/HyperLogLog>`_
- `MinHash with a single hash function <http://wikipedia.org/wiki/MinHash#Variant_with_a_single_hash_function>`_

As of now, ``HyperLogLog`` and ``MinHash`` are among the techniques implemented in Presto or used
by certain functions in Presto to handle large data sets.

``HyperLogLog (HLL)``: HyperLogLog is an algorithm used to estimate the cardinality
of a set — that is, the number of distinct elements in a large data set.
Presto uses it to provide the function approx_distinct which can be used to estimate the number
of distinct entries in a column.

Examples::

        SELECT approx_distinct(column_name) FROM table_name;

``MinHash``: MinHash is used to estimate the similarity between two or more sets, commonly known as Jaccard similarity.
It is particularly effective when dealing with large data sets and is generally used in data clustering
and near-duplicate detection.

Examples::

        WITH mh1 AS (SELECT minhash_agg(to_utf8(value)) AS minhash FROM table1), mh2 AS (SELECT minhash_agg(to_utf8(value))
        AS minhash FROM table2), SELECT jaccard_index(mh1.minhash, mh2.minhash) AS similarity FROM mh1, mh2;

The Presto type for this data structure is called ``setdigest``.
Presto offers the ability to merge multiple Set Digest data sketches.

Serialization
-------------

Data sketches such as those created via the use of MinHash or HyperLogLog can be serialized into a varbinary data type.
Serializing these data structures allows them to be efficiently stored and, if needed, transferred between different
systems or sessions.
Once stored, they can then be deserialized back into to their original state when they need to be used again.
In the context of Presto, you might normally do this using functions that convert these data sketches to and from binary.
An example might include using ``to_utf8()`` or ``from_utf8()``.

Functions
---------

.. function:: make_set_digest(x) -> setdigest

Composes all input values of ``x`` into a ``setdigest``.

    Examples::

        Create a ``setdigest`` corresponding to a ``bigint`` array::

        SELECT make_set_digest(value)
        FROM (VALUES 1, 2, 3) T(value);

        Create a ``setdigest`` corresponding to a ``varchar`` array::

        SELECT make_set_digest(value)
        FROM (VALUES 'Presto', 'SQL', 'on', 'everything') T(value);


.. function:: merge_set_digest(setdigest) -> setdigest

Returns the ``setdigest`` of the aggregate union of the individual ``setdigest`` structures.

     Examples::

        SELECT merge_set_digest(a) from (SELECT make_set_digest(value) as a FROM (VALUES 4,3,2,1) T(value));

.. function:: cardinality(setdigest) -> bigint

Returns the cardinality of the set digest from its internal
``HyperLogLog`` component.

    Examples::

        SELECT cardinality(make_set_digest(value))
        FROM (VALUES 1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5) T(value);
        -- 5

.. function:: intersection_cardinality(x,y) -> bigint

Returns the estimation for the cardinality of the intersection of the two set digests.

``x`` and ``y``  be of type  ``setdigest``

    Examples::

        SELECT intersection_cardinality(make_set_digest(v1), make_set_digest(v2))
        FROM (VALUES (1, 1), (NULL, 2), (2, 3), (3, 4)) T(v1, v2);
        -- 3

.. function:: jaccard_index(x, y) -> double

Returns the estimation of `Jaccard index <https://wikipedia.org/wiki/Jaccard_index>`_ for
the two set digests.

``x`` and ``y`` be of type  ``setdigest``.

    Examples::

        SELECT jaccard_index(make_set_digest(v1), make_set_digest(v2))
        FROM (VALUES (1, 1), (NULL,2), (2, 3), (NULL, 4)) T(v1, v2);
        -- 0.5

.. function:: hash_counts(x) -> map(bigint, smallint)

Returns a map containing the `Murmur3Hash128 <https://wikipedia.org/wiki/MurmurHash#MurmurHash3>`_
hashed values and the count of their occurences within
the internal ``MinHash`` structure belonging to ``x`` or varchar

``x`` must be of type  ``setdigest``.

    Examples::

        SELECT hash_counts(make_set_digest(value))
        FROM (VALUES 1, 1, 1, 2, 2) T(value);
        -- {19144387141682250=3, -2447670524089286488=2}
