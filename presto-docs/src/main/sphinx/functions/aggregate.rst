===================
Aggregate Functions
===================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

Aggregate functions operate on a set of values to compute a single result.

Except for :func:`!count`, :func:`!count_if`, :func:`!max_by`, :func:`!min_by` and
:func:`!approx_distinct`, all of these aggregate functions ignore null values
and return null for no input rows or when all values are null. For example,
:func:`!sum` returns null rather than zero and :func:`!avg` does not include null
values in the count. The ``coalesce`` function can be used to convert null into
zero.

Some aggregate functions such as :func:`!array_agg` produce different results
depending on the order of input values. This ordering can be specified by writing
an :ref:`order-by-clause` within the aggregate function::

    array_agg(x ORDER BY y DESC)
    array_agg(x ORDER BY x, y, z)


General Aggregate Functions
---------------------------

.. function:: any_value(x) -> [same as input]

    This is an alias for :func:`!arbitrary`.
    ::

       SELECT any_value(t1.age),t1.gender
       FROM
       (
        SELECT *
        FROM
        (
            VALUES
                ('Alice', 30,'male'),
                ('Bob', 25,'male'),
                ('Charlie', 22,'female'),
                ('Lucy', 20,'female')
        ) AS t(name, age, gender)) t1
       group by t1.gender;
       --(22,female)
       --(30,male)

.. function:: arbitrary(x) -> [same as input]

    Returns an arbitrary non-null value of ``x``, if one exists.

.. function:: array_agg(x) -> array<[same as input]>

    Returns an array created from the input ``x`` elements.
    ::

        SELECT array_agg(name)
        FROM
        (
            VALUES
                ('Alice', 30,'male'),
                ('Bob', 25,'male'),
                ('Charlie', 22,'female'),
                ('Lucy', 20,'female')
        ) AS t(name, age, gender);
        --['Alice','Bob','Charlie','Lucy']

.. function:: avg(x) -> double

    Returns the average (arithmetic mean) of all input values.
    ::

       SELECT avg(age)
       FROM
       (
       	VALUES
           ('Alice', 30,'male'),
           ('Bob', 25,'male'),
           ('Charlie', 22,'female'),
           ('Lucy', 20,'female')
        ) AS t(name, age, gender);
        --(24.25)

.. function:: avg(time interval type) -> time interval type

    Returns the average interval length of all input values.
    ::

       SELECT avg(timediff)
       FROM
       (
       	VALUES
           (INTERVAL '10' DAY),
           (INTERVAL '20' DAY),
           (INTERVAL '30' DAY)
        ) AS t(timediff);
        --(20 00:00:00.000)//INTERVAL '20' DAY

.. function:: bool_and(boolean) -> boolean

    Returns ``TRUE`` if every input value is ``TRUE``, otherwise ``FALSE``.
    ::

       SELECT bool_and(true_or_false)
       FROM
       (
       	VALUES
           (true),
           (true),
           (false)
        ) AS t(true_or_false);
        --(false)

.. function:: bool_or(boolean) -> boolean

    Returns ``TRUE`` if any input value is ``TRUE``, otherwise ``FALSE``.
    ::

       SELECT bool_or(true_or_false)
       FROM
       (
       	VALUES
           (true),
           (true),
           (false)
        ) AS t(true_or_false);
        --(true)

.. function:: checksum(x) -> varbinary

    Returns an order-insensitive checksum of the given values.
    ::

       SELECT checksum(name)
       FROM
       (
       	VALUES
           ('Alice', 30,'male'),
           ('Bob', 25,'male'),
           ('Charlie', 22,'female'),
           ('Lucy', 20,'female')
        ) AS t(name, age, gender);
        --(C0ACD56CF866E759)//hex format

       SELECT checksum(name)
       FROM
       (
       	VALUES
           ('Alice', 30,'male'),
           ('Bob', 25,'male'),
           ('Lucy', 20,'female'),
           ('Charlie', 22,'female')
        ) AS t(name, age, gender);
        --(C0ACD56CF866E759)//hex format

.. function:: count(*) -> bigint

    Returns the number of input rows.
    ::

       SELECT count(*)
       FROM
       (
       	VALUES
           ('Alice', 30,'male'),
           ('Bob', 25,'male'),
           ('Charlie', 22,'female'),
           ('Lucy', 20,'female')
        ) AS t(name, age, gender);
        --(4)

       SELECT count(*)
       FROM
       (
       	VALUES
           ('Alice', 30,'male'),
           ('Bob', 25,'male'),
           ('Charlie', 22,'female'),
           ('Lucy', null,'female')
        ) AS t(name, age, gender);
        --(4)

.. function:: count(x) -> bigint

    Returns the number of non-null input values.
    ::

       SELECT count(age)
       FROM
       (
       	VALUES
           ('Alice', 30,'male'),
           ('Bob', 25,'male'),
           ('Charlie', 22,'female'),
           ('Lucy', 20,'female')
        ) AS t(name, age, gender);
        --(4)

       SELECT count(age)
       FROM
       (
       	VALUES
           ('Alice', 30,'male'),
           ('Bob', 25,'male'),
           ('Charlie', 22,'female'),
           ('Lucy', null,'female')
        ) AS t(name, age, gender);
        --(3)

.. function:: count_if(x) -> bigint

    Returns the number of ``TRUE`` input values.
    This function is equivalent to ``count(CASE WHEN x THEN 1 END)``.
    ::

        SELECT count_if(gender = 'female') AS female_count
        FROM (
        VALUES
            ('Alice', 30, 'female'),
            ('Bob', 25, 'male'),
            ('Lucy', 22, 'female')
        ) AS t(name, age, gender);
        --(2)

.. function:: every(boolean) -> boolean

    This is an alias for :func:`!bool_and`.

.. function:: geometric_mean(bigint) -> double
              geometric_mean(double) -> double
              geometric_mean(real) -> real

    Returns the `geometric mean <https://en.wikipedia.org/wiki/Geometric_mean>`_ of all input values.
    ::

        SELECT geometric_mean(age) AS geo_mean_age
        FROM (
            VALUES
                ('Alice', 30, 'female'),
                ('Bob', 25, 'male'),
                ('Lucy', 22, 'female'),
                ('Tom', 28, 'male')
        ) AS t(name, age, gender);
        --(26.07116834203365)

.. function:: max_by(x, y) -> [same as x]

    Returns the value of ``x`` associated with the maximum value of ``y`` over all input values.
    ::

        SELECT max_by(name, age) AS oldest_person
        FROM (
            VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22),
            ('Tom', 35)
        ) AS t(name, age);
        --(Tom)

.. function:: max_by(x, y, n) -> array<[same as x]>

    Returns ``n`` values of ``x`` associated with the ``n`` largest of all input values of ``y``
    in descending order of ``y``.
    ::

        SELECT max_by(name, age, 2) AS top_2_oldest
        FROM (
            VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22),
            ('Tom', 35),
            ('Jerry', 33)
        ) AS t(name, age);
        --[Tom,Jerry]

.. function:: min_by(x, y) -> [same as x]

    Returns the value of ``x`` associated with the minimum value of ``y`` over all input values.
    ::

        SELECT min_by(name, age) AS youngest_person
        FROM (
        VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22),
            ('Tom', 35)
        ) AS t(name, age);
        --(Lucy)

.. function:: min_by(x, y, n) -> array<[same as x]>

    Returns ``n`` values of ``x`` associated with the ``n`` smallest of all input values of ``y``
    in ascending order of ``y``.
    ::

        SELECT min_by(name, age,2) AS youngest_person
        FROM (
        VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22),
            ('Tom', 35)
        ) AS t(name, age);
        --[Lucy,Bob]

.. function:: max(x) -> [same as input]

    Returns the maximum value of all input values.
    ::

        SELECT max(age) AS max_age
        FROM (
        VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22),
            ('Tom', 35)
        ) AS t(name, age);
        --(35)

.. function:: max(x, n) -> array<[same as x]>

    Returns ``n`` largest values of all input values of ``x``.
    ::

        SELECT max(age, 3) AS top_3_ages
        FROM (
        VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22),
            ('Tom', 35),
            ('Jerry', 33)
        ) AS t(name, age);
        --[35,33,30]

.. function:: min(x) -> [same as input]

    Returns the minimum value of all input values.
    ::

        SELECT min(age) AS min_age
        FROM (
        VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22),
            ('Tom', 35)
        ) AS t(name, age);
        --(22)

.. function:: min(x, n) -> array<[same as x]>

    Returns ``n`` smallest values of all input values of ``x``.
    ::

        SELECT min(age, 3) AS bottom_3_ages
        FROM (
        VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22),
            ('Tom', 35),
            ('Jerry', 33)
        ) AS t(name, age);
        --[22,25,30]

.. function:: reduce_agg(inputValue T, initialState S, inputFunction(S,T,S), combineFunction(S,S,S)) -> S

    Reduces all input values into a single value. ``inputFunction`` will be invoked
    for each input value. In addition to taking the input value, ``inputFunction``
    takes the current state, initially ``initialState``, and returns the new state.
    ``combineFunction`` will be invoked to combine two states into a new state.
    The final state is returned. Throws an error if ``initialState`` is NULL.
    The behavior is undefined if ``inputFunction`` or ``combineFunction`` return a NULL.

    Take care when designing ``initialState``, ``inputFunction`` and ``combineFunction``.
    These must support evaluating aggregation in a distributed manner using partial
    aggregation on many nodes, followed by shuffle over group-by keys, followed by
    final aggregation. Consider all possible values of state to ensure that
    ``combineFunction`` is `commutative <https://en.wikipedia.org/wiki/Commutative_property>`_
    and `associative <https://en.wikipedia.org/wiki/Associative_property>`_
    operation with ``initialState`` as the
    `identity <https://en.wikipedia.org/wiki/Identity_element>`_ value.::

        combineFunction(s, initialState) = s for any s

        combineFunction(s1, s2) = combineFunction(s2, s1) for any s1 and s2

        combineFunction(s1, combineFunction(s2, s3)) = combineFunction(combineFunction(s1, s2), s3) for any s1, s2, s3

    In addition, make sure that the following holds for the inputFunction::

        inputFunction(inputFunction(initialState, x), y) = combineFunction(inputFunction(initialState, x), inputFunction(initialState, y)) for any x and y

    ::

        SELECT id, reduce_agg(value, 0, (a, b) -> a + b, (a, b) -> a + b)
        FROM (
            VALUES
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 20),
                (2, 30),
                (2, 40)
        ) AS t(id, value)
        GROUP BY id;
        -- (1, 9)
        -- (2, 90)

        SELECT id, reduce_agg(value, 1, (a, b) -> a * b, (a, b) -> a * b)
        FROM (
            VALUES
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 20),
                (2, 30),
                (2, 40)
        ) AS t(id, value)
        GROUP BY id;
        -- (1, 24)
        -- (2, 24000)

    The state type must be a boolean, integer, floating-point, or date/time/interval.

.. function:: set_agg(x) -> array<[same as input]>

    Returns an array created from the distinct input ``x`` elements.

    If the input includes ``NULL``, ``NULL`` will be included in the returned array.
    If the input includes arrays with ``NULL`` elements or rows with ``NULL`` fields, they will
    be included in the returned array.  This function uses ``IS DISTINCT FROM`` to determine
    distinctness. ::

        SELECT set_agg(x) FROM (VALUES(1), (2), (null), (2), (null)) t(x) -- ARRAY[1, 2, null]
        SELECT set_agg(x) FROM (VALUES(ROW(ROW(1, null))), ROW((ROW(2, 'a'))), ROW((ROW(1, null))), (null)) t(x) -- ARRAY[ROW(1, null), ROW(2, 'a'), null]


.. function:: set_union(array(T)) -> array(T)

    Returns an array of all the distinct values contained in each array of the input.

    When all inputs are ``NULL``, this function returns an empty array. If ``NULL`` is
    an element of one of the input arrays, ``NULL`` will be included in the returned array.
    If the input includes arrays with ``NULL`` elements or rows with ``NULL`` fields, they will
    be included in the returned array.  This function uses ``IS DISTINCT FROM`` to determine
    distinctness.

    Example::

        SELECT set_union(elements)
        FROM (
            VALUES
                ARRAY[1, 2, 3],
                ARRAY[2, 3, 4]
        ) AS t(elements);

    Returns ARRAY[1, 2, 3, 4]

.. function:: sum(x) -> [same as input]

    Returns the sum of all input values.

Bitwise Aggregate Functions
---------------------------

.. function:: bitwise_and_agg(x) -> bigint

    Returns the bitwise AND of all input values in 2's complement representation.
    ::

        SELECT bitwise_and_agg(flags) AS result
        FROM (
        VALUES
            (7),   -- 0b0111
            (3),   -- 0b0011
            (1)    -- 0b0001
        ) AS t(flags);
        --(1) //0b0001

.. function:: bitwise_or_agg(x) -> bigint

    Returns the bitwise OR of all input values in 2's complement representation.
    ::

        SELECT bitwise_or_agg(flags) AS result
        FROM (
        VALUES
            (7),   -- 0b0111
            (3),   -- 0b0011
            (1)    -- 0b0001
        ) AS t(flags);
        --(7) //0b0111

.. function:: bitwise_xor_agg(x) -> bigint

    Returns the bitwise XOR of all input values in 2's complement representation.
    ::

        SELECT bitwise_xor_agg(flags) AS result
        FROM (
        VALUES
            (7),   -- 0b0111
            (3),   -- 0b0011
            (1)    -- 0b0001
        ) AS t(flags);
        --(5) //0b0101

Map Aggregate Functions
-----------------------

.. function:: histogram(x) -> map(K,bigint)

    Returns a map containing the count of the number of times each input value occurs.
    ::

        SELECT histogram(age) AS age_histogram
        FROM (
        VALUES
            (30),
            (25),
            (30),
            (22),
            (25),
            (30)
        ) AS t(age);
        --{22->1, 25->2, 30->3}

.. function:: map_agg(key, value) -> map(K,V)

    Returns a map created from the input ``key`` / ``value`` pairs.
    ::

        SELECT map_agg(name, age) AS name_age_map
        FROM (
        VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Lucy', 22)
        ) AS t(name, age);
        --{Bob->25, Alice->30, Lucy->22}

.. function:: map_union(x(K,V)) -> map(K,V)

   Returns the union of all the input maps. If a key is found in multiple
   input maps, that key's value in the resulting map comes from an arbitrary input map.
   ::

        SELECT map_union(maps) AS merged_map
        FROM (
        VALUES
            (MAP(ARRAY['a', 'b'], ARRAY[1, 2])),
            (MAP(ARRAY['b', 'c'], ARRAY[3, 4])),
            (MAP(ARRAY['d'], ARRAY[5]))
        ) AS t(maps);
        --{a->1, b->2, c->4, d->5}

.. function:: map_union_sum(x(K,V)) -> map(K,V)

      Returns the union of all the input maps summing the values of matching keys in all
      the maps. All null values in the original maps are coalesced to 0.
      ::

            SELECT map_union_sum(maps) AS merged_sum_map
            FROM (
            VALUES
                (MAP(ARRAY['a', 'b'], ARRAY[1, 2])),
                (MAP(ARRAY['b', 'c'], ARRAY[3, 4])),
                (MAP(ARRAY['a', 'd'], ARRAY[5, 6]))
            ) AS t(maps);
            --{'a'->6,'b'->5,'c'->4,'d'->6}

.. function:: multimap_agg(key, value) -> map(K,array(V))

    Returns a multimap created from the input ``key`` / ``value`` pairs.
    Each key can be associated with multiple values.

    ::

        SELECT multimap_agg(name, age) AS name_age_multimap
        FROM (
        VALUES
            ('Alice', 30),
            ('Bob', 25),
            ('Alice', 32),
            ('Lucy', 22),
            ('Bob', 28)
        ) AS t(name, age);
        --{Bob->[25, 28], Alice->[30, 32], Lucy->[22]}

Approximate Aggregate Functions
-------------------------------

.. function:: approx_distinct(x) -> bigint

    Returns the approximate number of distinct input values.
    This function provides an approximation of ``count(DISTINCT x)``.
    Zero is returned if all input values are null.

    This function should produce a standard error of 2.3%, which is the
    standard deviation of the (approximately normal) error distribution over
    all possible sets. It does not guarantee an upper bound on the error for
    any specific input set.
    ::

        SELECT approx_distinct(name) AS distinct_names
        FROM (
        VALUES
            ('Alice'),
            ('Bob'),
            ('Alice'),
            ('Lucy'),
            ('Bob'),
            ('Tom')
        ) AS t(name);
        --(4)

.. function:: approx_distinct(x, e) -> bigint

    Returns the approximate number of distinct input values.
    This function provides an approximation of ``count(DISTINCT x)``.
    Zero is returned if all input values are null.

    This function should produce a standard error of no more than ``e``, which
    is the standard deviation of the (approximately normal) error distribution
    over all possible sets. It does not guarantee an upper bound on the error
    for any specific input set. The current implementation of this function
    requires that ``e`` be in the range of ``[0.0040625, 0.26000]``.
    ::

        SELECT approx_distinct(gender, 0.01) AS estimated_distinct_gender
        FROM (
        VALUES 
            ('Alice', 30, 'female'),
            ('Bob', 25, 'male'),
            ('Lucy', 22, 'female'),
            ('Tom', 40, 'male'),
            ('Amy', 35, 'female')
        ) AS t(name, age, gender);
        --(2)

.. function:: approx_percentile(x, percentage) -> [same as x]

    Returns the approximate percentile for all input values of ``x`` at the
    given ``percentage``. The value of ``percentage`` must be between zero and
    one and must be constant for all input rows.
    ::

        SELECT approx_percentile(age, 0.5) AS median_age
        FROM (
        VALUES
            (30),
            (25),
            (22),
            (35),
            (33),
            (28)
        ) AS t(age);
        --(30)

.. function:: approx_percentile(x, percentage, accuracy) -> [same as x]

    As ``approx_percentile(x, percentage)``, but with a maximum rank error of
    ``accuracy``. The value of ``accuracy`` must be between zero and one
    (exclusive) and must be constant for all input rows. Note that a lower
    "accuracy" is really a lower error threshold, and thus more accurate. The
    default accuracy is ``0.01``.
    ::

        SELECT approx_percentile(age, 0.5, 0.9) AS median_age
        FROM (
        VALUES
            (30),
            (25),
            (22),
            (35),
            (33),
            (28)
        ) AS t(age);
        --(30)

.. function:: approx_percentile(x, percentages) -> array<[same as x]>

    Returns the approximate percentile for all input values of ``x`` at each of
    the specified percentages. Each element of the ``percentages`` array must be
    between zero and one, and the array must be constant for all input rows.
    ::

        SELECT approx_percentile(age, ARRAY[0.25, 0.5, 0.75]) AS percentiles
        FROM (
        VALUES
            (22),
            (25),
            (28),
            (30),
            (33),
            (35)
        ) AS t(age);
        --[25,30,33]

.. function:: approx_percentile(x, percentages, accuracy) -> array<[same as x]>

    As ``approx_percentile(x, percentages)``, but with a maximum rank error of
    ``accuracy``.
    ::

        SELECT approx_percentile(age, ARRAY[0.25, 0.5, 0.75], 0.9) AS percentiles
        FROM (
        VALUES
            (22),
            (25),
            (28),
            (30),
            (33),
            (35)
        ) AS t(age);
        --[25,30,33]

.. function:: approx_percentile(x, w, percentage) -> [same as x]

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at the percentage ``p``. The weight must be
    an integer value of at least one. It is effectively a replication count for
    the value ``x`` in the percentile set. The value of ``p`` must be between
    zero and one and must be constant for all input rows.
    ::

        SELECT approx_percentile(age, weight, 0.5) AS weighted_median
        FROM (
        VALUES
            (22, 1),
            (25, 2),
            (28, 1),
            (30, 3),
            (33, 1),
            (35, 2)
        ) AS t(age, weight);
        --(30)

.. function:: approx_percentile(x, w, percentage, accuracy) -> [same as x]

    As ``approx_percentile(x, w, percentage)``, but with a maximum rank error of
    ``accuracy``.
    ::

        SELECT approx_percentile(age, weight, 0.5, 0.9) AS weighted_median
        FROM (
        VALUES
            (22, 1),
            (25, 2),
            (28, 1),
            (30, 3),
            (33, 1),
            (35, 2)
        ) AS t(age, weight);
        --(30)

.. function:: approx_percentile(x, w, percentages) -> array<[same as x]>

    Returns the approximate weighed percentile for all input values of ``x``
    using the per-item weight ``w`` at each of the given percentages specified
    in the array. The weight must be an integer value of at least one. It is
    effectively a replication count for the value ``x`` in the percentile set.
    Each element of the array must be between zero and one, and the array must
    be constant for all input rows.
    ::

        SELECT approx_percentile(age, weight, ARRAY[0.25, 0.5, 0.75]) AS weighted_percentiles
        FROM (
        VALUES
            (22, 1),
            (25, 2),
            (28, 1),
            (30, 3),
            (33, 1),
            (35, 2)
        ) AS t(age, weight);
        -[25,30,33]

.. function:: approx_percentile(x, w, percentages, accuracy) -> array<[same as x]>

    As ``approx_percentile(x, w, percentages)``, but with a maximum rank error of
    ``accuracy``.
    ::

        SELECT approx_percentile(age, weight, ARRAY[0.25, 0.5, 0.75],0.9) AS weighted_percentiles
        FROM (
        VALUES
            (22, 1),
            (25, 2),
            (28, 1),
            (30, 3),
            (33, 1),
            (35, 2)
        ) AS t(age, weight);
        -[25,30,33]

.. function:: approx_set(x) -> HyperLogLog
    :noindex:

    See :doc:`hyperloglog`.
    ::

        SELECT approx_set(user_id) AS hll_data
        FROM (
        VALUES
            (1001),
            (1002),
            (1003),
            (1001),
            (1004)
        ) AS t(user_id);
        --(020C0400401E4D1D4081707280E083BD444759E9)//hex format

.. function:: merge(x) -> HyperLogLog
    :noindex:

    See :doc:`hyperloglog`.
    ::

        WITH hll_data AS (
        SELECT region, approx_set(user_id) AS hll
        FROM (
            VALUES
            ('east', 1),
            ('east', 2),
            ('west', 2),
            ('west', 3),
            ('west', 4)
        ) AS t(region, user_id)
        GROUP BY region
        )
        SELECT cardinality(merge(hll)) AS total_unique_users
        FROM hll_data;
        --(4)

.. function:: khyperloglog_agg(x) -> KHyperLogLog
    :noindex:

    See :doc:`khyperloglog`.
    ::

        SELECT cardinality(khyperloglog_agg(x,y))
        FROM (
        VALUES (1,101), (2,102), (3,103), (4,101), (5,104)
        ) AS t(x,y);
        --(5)

.. function:: merge(qdigest(T)) -> qdigest(T)
    :noindex:

    See :doc:`qdigest`.

.. function:: qdigest_agg(x) -> qdigest<[same as x]>
    :noindex:

    See :doc:`qdigest`.

.. function:: qdigest_agg(x, w) -> qdigest<[same as x]>
    :noindex:

    See :doc:`qdigest`.

.. function:: qdigest_agg(x, w, accuracy) -> qdigest<[same as x]>
    :noindex:

    See :doc:`qdigest`.

.. function:: numeric_histogram(buckets, value, weight) -> map<double, double>

    Computes an approximate histogram with up to ``buckets`` number of buckets
    for all ``value``\ s with a per-item weight of ``weight``.  The keys of the
    returned map are roughly the center of the bin, and the entry is the total
    weight of the bin.  The algorithm is based loosely on [BenHaimTomTov2010]_.

    ``buckets`` must be a ``bigint``. ``value`` and ``weight`` must be numeric.
    ::

        SELECT numeric_histogram(3, v, 1.0)
        FROM (
         VALUES (10),
                (15),
                (20),
                (25),
                (30)
        ) AS t(v);
        --{30.0->1.0, 22.5->2.0, 12.5->2.0}

.. function:: numeric_histogram(buckets, value) -> map<double, double>

    Computes an approximate histogram with up to ``buckets`` number of buckets
    for all ``value``\ s. This function is equivalent to the variant of
    :func:`!numeric_histogram` that takes a ``weight``, with a per-item weight of ``1``.
    In this case, the total weight in the returned map is the count of items in the bin.
    ::

        SELECT numeric_histogram(3, v)
        FROM (
        VALUES (10.0), (15.0), (20.0), (25.0), (30.0)
        ) AS t(v);
        --{30.0->1.0, 22.5->2.0, 12.5->2.0}

Statistical Aggregate Functions
-------------------------------

.. function:: corr(y, x) -> double

    Returns correlation coefficient of input values.
    ::

        SELECT corr(score, study_hours)
        FROM (
        VALUES 
            (85, 2),
            (90, 3),
            (95, 4),
            (70, 1),
            (80, 2)
        ) AS t(score, study_hours);
        --(0.95751756)

.. function:: covar_pop(y, x) -> double

    Returns the population covariance of input values.
    ::

        SELECT covar_pop(score, study_hours)
        FROM (
        VALUES 
            (85, 2),
            (90, 3),
            (95, 4),
            (70, 1),
            (80, 2)
        ) AS t(score, study_hours);
        --(8.4)

.. function:: covar_samp(y, x) -> double

    Returns the sample covariance of input values.
    ::

        SELECT covar_samp(score, hours)
        FROM (
        VALUES 
            (85, 2),
            (90, 3),
            (95, 4),
            (70, 1),
            (80, 2)
        ) AS t(score, hours);
        --(10.5)

.. function:: entropy(c) -> double

    Returns the log-2 entropy of count input-values.

    .. math::

        \mathrm{entropy}(c) = \sum_i \left[ {c_i \over \sum_j [c_j]} \log_2\left({\sum_j [c_j] \over c_i}\right) \right].

    ``c`` must be a ``bigint`` column of non-negative values.

    The function ignores any ``NULL`` count. If the sum of non-``NULL`` counts is 0,
    it returns 0.
    ::

        SELECT entropy(category)
        FROM (
        VALUES 
            (1),
            (1),
            (2),
            (1),
            (2)
        ) AS t(category);
        --(2.2359263506290326)

.. function:: kurtosis(x) -> double

    Returns the excess kurtosis of all input values. Unbiased estimate using
    the following expression:

    .. math::

        \mathrm{kurtosis}(x) = {n(n+1) \over (n-1)(n-2)(n-3)} { \sum[(x_i-\mu)^4] \over \sigma^4} -3{ (n-1)^2 \over (n-2)(n-3) }

    where :math:`\mu` is the mean, and :math:`\sigma` is the standard deviation.
    ::

        SELECT kurtosis(salary)
        FROM (
        VALUES (1000),
                (1200),
                (1100),
                (1500),
                (900),
                (2500)
        ) AS t(salary);
        --(3.549458572481891)

.. function:: regr_intercept(y, x) -> double

    Returns linear regression intercept of input values. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_intercept(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (40, 6000)
        ) AS t(age, salary);
        --(-2000)

.. function:: regr_slope(y, x) -> double

    Returns linear regression slope of input values. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_slope(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (40, 6000)
        ) AS t(age, salary);
        --(200)

.. function:: regr_avgx(y, x) -> double

    Returns the average of the independent value in a group. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_avgx(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (NULL, 6000),
            (40, NULL)
        ) AS t(age, salary);
        --(30)

.. function:: regr_avgy(y, x) -> double

    Returns the average of the dependent value in a group. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_avgy(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (NULL, 6000),
            (40, NULL)
        ) AS t(age, salary);
        --(4000)

.. function:: regr_count(y, x) -> double

    Returns the number of non-null pairs of input values. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_count(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (NULL, 6000),
            (40, NULL)
        ) AS t(age, salary);
        --(3)


.. function:: regr_r2(y, x) -> double

    Returns the coefficient of determination of the linear regression. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_r2(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (NULL, 6000),
            (40, NULL)
        ) AS t(age, salary);
        --(1)

.. function:: regr_sxy(y, x) -> double

    Returns the sum of the product of the dependent and independent values in a group. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_sxy(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (NULL, 6000),
            (40, NULL)
        ) AS t(age, salary);
        --(10000)

.. function:: regr_syy(y, x) -> double

    Returns the sum of the squares of the dependent values in a group. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_syy(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (NULL, 6000),
            (40, NULL)
        ) AS t(age, salary);
        --(2000000)

.. function:: regr_sxx(y, x) -> double

    Returns the sum of the squares of the independent values in a group. ``y`` is the dependent
    value. ``x`` is the independent value.
    ::

        SELECT regr_sxx(salary, age)
        FROM (
        VALUES 
            (25, 3000),
            (30, 4000),
            (35, 5000),
            (NULL, 6000),
            (40, NULL)
        ) AS t(age, salary);
        --(50)

.. function:: skewness(x) -> double

    Returns the skewness of all input values.
    ::

        SELECT skewness(salary)
        FROM (
        VALUES 
            (3000),
            (4000),
            (5000),
            (6000),
            (10000)
        ) AS t(salary);
        --(0.8978957037987336)

.. function:: stddev(x) -> double

    This is an alias for :func:`!stddev_samp`.
    ::

        SELECT stddev(salary)
        FROM (
        VALUES 
            (3000),
            (4000),
            (5000),
            (6000),
            (7000)
        ) AS t(salary);
        --(1581.1388300841897)

.. function:: stddev_pop(x) -> double

    Returns the population standard deviation of all input values.
    ::

        SELECT stddev_pop(salary)
        FROM (
        VALUES 
            (3000),
            (4000),
            (5000),
            (6000),
            (7000)
        ) AS t(salary);
        --(1414.213562373095)

.. function:: stddev_samp(x) -> double

    Returns the sample standard deviation of all input values.
    ::

        SELECT stddev_samp(salary)
        FROM (
        VALUES 
            (3000),
            (4000),
            (5000),
            (6000),
            (7000)
        ) AS t(salary);
        --(1581.1388300841897)

.. function:: variance(x) -> double

    This is an alias for :func:`!var_samp`.
    ::

        SELECT variance(salary)
        FROM (
        VALUES 
            (3000),
            (4000),
            (5000),
            (6000),
            (7000)
        ) AS t(salary);
        --(2500000.0)

.. function:: var_pop(x) -> double

    Returns the population variance of all input values.
    ::

        SELECT var_pop(salary)
        FROM (
        VALUES 
            (3000),
            (4000),
            (5000),
            (6000),
            (7000)
        ) AS t(salary);
        --(2000000.0)

.. function:: var_samp(x) -> double

    Returns the sample variance of all input values.
    ::

        SELECT var_samp(salary)
        FROM (
        VALUES 
            (3000),
            (4000),
            (5000),
            (6000),
            (7000)
        ) AS t(salary);
        --(2500000.0)

Classification Metrics Aggregate Functions
------------------------------------------

The following functions each measure how some metric of a binary
`confusion matrix <https://en.wikipedia.org/wiki/Confusion_matrix>`_ changes as a function of
classification thresholds. They are meant to be used in conjunction.

For example, to find the `precision-recall curve <https://en.wikipedia.org/wiki/Precision_and_recall>`_, use

.. code-block:: none

    WITH
        recall_precision AS (
            SELECT
                CLASSIFICATION_RECALL(10000, correct, pred) AS recalls,
                CLASSIFICATION_PRECISION(10000, correct, pred) AS precisions
            FROM
                classification_dataset
        )
    SELECT
        recall,
        precision
    FROM
        recall_precision
    CROSS JOIN UNNEST(recalls, precisions) AS t(recall, precision)

To get the corresponding thresholds for these values, use

.. code-block:: none

    WITH
        recall_precision AS (
            SELECT
                CLASSIFICATION_THRESHOLDS(10000, correct, pred) AS thresholds,
                CLASSIFICATION_RECALL(10000, correct, pred) AS recalls,
                CLASSIFICATION_PRECISION(10000, correct, pred) AS precisions
            FROM
                classification_dataset
        )
    SELECT
        threshold,
        recall,
        precision
    FROM
        recall_precision
    CROSS JOIN UNNEST(thresholds, recalls, precisions) AS t(threshold, recall, precision)

To find the `ROC curve <https://en.wikipedia.org/wiki/Receiver_operating_characteristic>`_, use

.. code-block:: none

    WITH
        fallout_recall AS (
            SELECT
                CLASSIFICATION_FALLOUT(10000, correct, pred) AS fallouts,
                CLASSIFICATION_RECALL(10000, correct, pred) AS recalls
            FROM
                classification_dataset
        )
    SELECT
        fallout
        recall,
    FROM
        recall_fallout
    CROSS JOIN UNNEST(fallouts, recalls) AS t(fallout, recall)


.. function:: classification_miss_rate(buckets, y, x, weight) -> array<double>

    Computes the miss-rate with up to ``buckets`` number of buckets. Returns
    an array of miss-rate values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1; ``weight`` should be non-negative values, indicating the weight of the instance.

    The
    `miss-rate <https://en.wikipedia.org/wiki/Type_I_and_type_II_errors#False_positive_and_false_negative_rates>`_
    is defined as a sequence whose :math:`j`-th entry is

    .. math ::

        {
            \sum_{i \;|\; x_i \leq t_j \bigwedge y_i = 1} \left[ w_i \right]
            \over
            \sum_{i \;|\; x_i \leq t_j \bigwedge y_i = 1} \left[ w_i \right]
            +
            \sum_{i \;|\; x_i > t_j \bigwedge y_i = 1} \left[ w_i \right]
        },

    where :math:`t_j` is the :math:`j`-th smallest threshold,
    and :math:`y_i`, :math:`x_i`, and :math:`w_i` are the :math:`i`-th
    entries of ``y``, ``x``, and ``weight``, respectively.

.. function:: classification_miss_rate(buckets, y, x) -> array<double>

    This function is equivalent to the variant of
    :func:`!classification_miss_rate` that takes a ``weight``, with a per-item weight of ``1``.

.. function:: classification_fall_out(buckets, y, x, weight) -> array<double>

    Computes the fall-out with up to ``buckets`` number of buckets. Returns
    an array of fall-out values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1; ``weight`` should be non-negative values, indicating the weight of the instance.

    The
    `fall-out <https://en.wikipedia.org/wiki/Information_retrieval#Fall-out>`_
    is defined as a sequence whose :math:`j`-th entry is

    .. math ::

        {
            \sum_{i \;|\; x_i > t_j \bigwedge y_i = 0} \left[ w_i \right]
            \over
            \sum_{i \;|\; y_i = 0} \left[ w_i \right]
        },

    where :math:`t_j` is the :math:`j`-th smallest threshold,
    and :math:`y_i`, :math:`x_i`, and :math:`w_i` are the :math:`i`-th
    entries of ``y``, ``x``, and ``weight``, respectively.

.. function:: classification_fall_out(buckets, y, x) -> array<double>

    This function is equivalent to the variant of
    :func:`!classification_fall_out` that takes a ``weight``, with a per-item weight of ``1``.

.. function:: classification_precision(buckets, y, x, weight) -> array<double>

    Computes the precision with up to ``buckets`` number of buckets. Returns
    an array of precision values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1; ``weight`` should be non-negative values, indicating the weight of the instance.

    The
    `precision <https://en.wikipedia.org/wiki/Positive_and_negative_predictive_values>`_
    is defined as a sequence whose :math:`j`-th entry is

    .. math ::

        {
            \sum_{i \;|\; x_i > t_j \bigwedge y_i = 1} \left[ w_i \right]
            \over
            \sum_{i \;|\; x_i > t_j} \left[ w_i \right]
        },

    where :math:`t_j` is the :math:`j`-th smallest threshold,
    and :math:`y_i`, :math:`x_i`, and :math:`w_i` are the :math:`i`-th
    entries of ``y``, ``x``, and ``weight``, respectively.

.. function:: classification_precision(buckets, y, x) -> array<double>

    This function is equivalent to the variant of
    :func:`!classification_precision` that takes a ``weight``, with a per-item weight of ``1``.

.. function:: classification_recall(buckets, y, x, weight) -> array<double>

    Computes the recall with up to ``buckets`` number of buckets. Returns
    an array of recall values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1; ``weight`` should be non-negative values, indicating the weight of the instance.

    The
    `recall <https://en.wikipedia.org/wiki/Precision_and_recall#Recall>`_
    is defined as a sequence whose :math:`j`-th entry is

    .. math ::

        {
            \sum_{i \;|\; x_i > t_j \bigwedge y_i = 1} \left[ w_i \right]
            \over
            \sum_{i \;|\; y_i = 1} \left[ w_i \right]
        },

    where :math:`t_j` is the :math:`j`-th smallest threshold,
    and :math:`y_i`, :math:`x_i`, and :math:`w_i` are the :math:`i`-th
    entries of ``y``, ``x``, and ``weight``, respectively.

.. function:: classification_recall(buckets, y, x) -> array<double>

    This function is equivalent to the variant of
    :func:`!classification_recall` that takes a ``weight``, with a per-item weight of ``1``.

.. function:: classification_thresholds(buckets, y, x) -> array<double>

    Computes the thresholds with up to ``buckets`` number of buckets. Returns
    an array of threshold values.

    ``y`` should be a boolean outcome value; ``x`` should be predictions, each
    between 0 and 1.

    The thresholds are defined as a sequence whose :math:`j`-th entry is the :math:`j`-th smallest threshold.


Differential Entropy Functions
-------------------------------

The following functions approximate the binary `differential entropy <https://en.wikipedia.org/wiki/Differential_entropy>`_.
That is, for a random variable :math:`x`, they approximate

.. math ::

    h(x) = - \int x \log_2\left(f(x)\right) dx,

where :math:`f(x)` is the partial density function of :math:`x`.

.. function:: differential_entropy(sample_size, x)

    Returns the approximate log-2 differential entropy from a random variable's sample outcomes. The function internally
    creates a reservoir (see [Black2015]_), then calculates the
    entropy from the sample results by approximating the derivative of the cumulative distribution
    (see [Alizadeh2010]_).

    ``sample_size`` (``long``) is the maximal number of reservoir samples.

    ``x`` (``double``) is the samples.

    For example, to find the differential entropy of ``x`` of ``data`` using 1000000 reservoir samples, use

    .. code-block:: none

        SELECT
            differential_entropy(1000000, x)
        FROM
            data

    .. note::

        If :math:`x` has a known lower and upper bound,
        prefer the versions taking ``(bucket_count, x, 1.0, "fixed_histogram_mle", min, max)``,
        or ``(bucket_count, x, 1.0, "fixed_histogram_jacknife", min, max)``,
        as they have better convergence.

.. function:: differential_entropy(sample_size, x, weight)

    Returns the approximate log-2 differential entropy from a random variable's sample outcomes. The function
    internally creates a weighted reservoir (see [Efraimidis2006]_), then calculates the
    entropy from the sample results by approximating the derivative of the cumulative distribution
    (see [Alizadeh2010]_).

    ``sample_size`` is the maximal number of reservoir samples.

    ``x`` (``double``) is the samples.

    ``weight`` (``double``) is a non-negative double value indicating the weight of the sample.

    For example, to find the differential entropy of ``x`` with weights ``weight`` of ``data``
    using 1000000 reservoir samples, use

    .. code-block:: none

         SELECT
             differential_entropy(1000000, x, weight)
         FROM
             data

    .. note::

        If :math:`x` has a known lower and upper bound,
        prefer the versions taking ``(bucket_count, x, weight, "fixed_histogram_mle", min, max)``,
        or ``(bucket_count, x, weight, "fixed_histogram_jacknife", min, max)``,
        as they have better convergence.

.. function:: differential_entropy(bucket_count, x, weight, method, min, max) -> double

    Returns the approximate log-2 differential entropy from a random variable's sample outcomes. The function
    internally creates a conceptual histogram of the sample values, calculates the counts, and
    then approximates the entropy using maximum likelihood with or without Jacknife
    correction, based on the ``method`` parameter. If Jacknife correction (see [Beirlant2001]_) is used, the
    estimate is

    .. math ::

        n H(x) - (n - 1) \sum_{i = 1}^n H\left(x_{(i)}\right)

    where :math:`n` is the length of the sequence, and :math:`x_{(i)}` is the sequence with the :math:`i`-th element
    removed.

    ``bucket_count`` (``long``) determines the number of histogram buckets.

    ``x`` (``double``) is the samples.

    ``method`` (``varchar``) is either ``'fixed_histogram_mle'`` (for the maximum likelihood estimate)
    or ``'fixed_histogram_jacknife'`` (for the jacknife-corrected maximum likelihood estimate).

    ``min`` and ``max`` (both ``double``) are the minimal and maximal values, respectively;
    the function will throw if there is an input outside this range.

    ``weight`` (``double``) is the weight of the sample, and must be non-negative.

    For example, to find the differential entropy of ``x``, each between ``0.0`` and ``1.0``,
    with weights 1.0 of ``data`` using 1000000 bins and jacknife estimates, use

    .. code-block:: none

         SELECT
             differential_entropy(1000000, x, 1.0, 'fixed_histogram_jacknife', 0.0, 1.0)
         FROM
             data

    To find the differential entropy of ``x``, each between ``-2.0`` and ``2.0``,
    with weights ``weight`` of ``data`` using 1000000 buckets and maximum-likelihood estimates, use

    .. code-block:: none

        SELECT
            differential_entropy(1000000, x, weight, 'fixed_histogram_mle', -2.0, 2.0)
        FROM
            data

    .. note::

        If :math:`x` doesn't have known lower and upper bounds, prefer the versions taking ``(sample_size, x)``
        (unweighted case) or ``(sample_size, x, weight)`` (weighted case), as they use reservoir
        sampling which doesn't require a known range for samples.

        Otherwise, if the number of distinct weights is low,
        especially if the number of samples is low, consider using the version taking
        ``(bucket_count, x, weight, "fixed_histogram_jacknife", min, max)``, as jacknife bias correction,
        is better than maximum likelihood estimation. However, if the number of distinct weights is high,
        consider using the version taking ``(bucket_count, x, weight, "fixed_histogram_mle", min, max)``,
        as this will reduce memory and running time.

.. function:: approx_most_frequent(buckets, value, capacity) -> map<[same as value], bigint>

    Computes the top frequent values up to ``buckets`` elements approximately.
    Approximate estimation of the function enables us to pick up the frequent
    values with less memory. Larger ``capacity`` improves the accuracy of
    underlying algorithm with sacrificing the memory capacity. The returned
    value is a map containing the top elements with corresponding estimated
    frequency.

    The error of the function depends on the permutation of the values and its
    cardinality. We can set the capacity same as the cardinality of the
    underlying data to achieve the least error.

    ``buckets`` and ``capacity`` must be ``bigint``. ``value`` can be numeric
    or string type.

    The function uses the stream summary data structure proposed in the paper
    `Efficient computation of frequent and top-k elements in data streams <https://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf>`_ by A.Metwally, D.Agrawal and A.Abbadi.

Reservoir Sample Functions
-------------------------------

Reservoir sample functions use a fixed sample size, as opposed to
:ref:`TABLESAMPLE <sql-tablesample>`. Fixed sample sizes always result in a
fixed total size while still guaranteeing that each record in dataset has an
equal probability of being chosen. See [Vitter1985]_.

.. function:: reservoir_sample(initial_sample: array(T), initial_processed_count: bigint, values_to_sample: T, desired_sample_size: int) -> row(processed_count: bigint, sample: array(T))

    Computes a new reservoir sample given:
    
    - ``initial_sample``: an initial sample array, or ``NULL`` if creating a new
      sample.
    - ``initial_processed_count``: the number of records processed to generate
      the initial sample array. This should be 0 or ``NULL`` if
      ``initital_sample`` is ``NULL``.
    - ``values_to_sample``: the column to sample from.
    - ``desired_sample_size``: the size of reservoir sample.

    The function outputs a single row type with two columns:

    #. Processed count: The total number of rows the function sampled
       from. It includes the total from the ``initial_processed_count``,
       if provided.

    #. Reservoir sample: An array with length equivalent to the minimum of
       ``desired_sample_size`` and the number of values in the
       ``values_to_sample`` argument.
    

    .. code-block:: sql

        WITH result as (
            SELECT
                reservoir_sample(NULL, 0, col, 5) as reservoir
            FROM (
                VALUES
                1, 2, 3, 4, 5, 6, 7, 8, 9, 0
            ) as t(col)
        )
        SELECT 
            reservoir.processed_count, reservoir.sample
        FROM result;

    .. code-block:: none

         processed_count |     sample
        -----------------+-----------------
                      10 | [1, 2, 8, 4, 5]

    To merge older samples with new data, supply valid arguments to the
    ``initial_sample`` argument and ``initial_processed_count`` arguments.

    .. code-block:: sql

        WITH initial_sample as (
            SELECT
                reservoir_sample(NULL, 0, col, 3) as reservoir
            FROM (
                VALUES
                0, 1, 2, 3, 4
            ) as t(col)
        ),
        new_sample as (
            SELECT
                reservoir_sample(
                    (SELECT reservoir.sample FROM initial_sample), 
                    (SELECT reservoir.processed_count FROM initial_sample), 
                    col, 
                    3
                ) as result
            FROM (
                VALUES
                5, 6, 7, 8, 9
            ) as t(col)
        )
        SELECT 
            result.processed_count, result.sample
        FROM new_sample;

    .. code-block:: none

         processed_count |  sample
        -----------------+-----------
                      10 | [8, 3, 2]

    To sample an entire row of a table, use a ``ROW`` type input with 
    each subfield corresponding to the columns of the source table.

    .. code-block:: sql

        WITH result as (
            SELECT
                reservoir_sample(NULL, 0, CAST(row(idx, val) AS row(idx int, val varchar)), 2) as reservoir
            FROM (
                VALUES
                (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')
            ) as t(idx, val)
        )
        SELECT 
            reservoir.processed_count, reservoir.sample
        FROM result;

    .. code-block:: none

         processed_count |              sample
        -----------------+----------------------------------
                       5 | [{idx=1, val=a}, {idx=5, val=e}]


Noisy Aggregate Functions
-------------------------

See :doc:`noisy`.


---------------------------

.. [Alizadeh2010] Alizadeh Noughabi, Hadi & Arghami, N. (2010). "A New Estimator of Entropy".

.. [Beirlant2001] Beirlant, Dudewicz, Gyorfi, and van der Meulen,
    "Nonparametric entropy estimation: an overview", (2001)

.. [BenHaimTomTov2010] Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
    J. Machine Learning Research 11 (2010), pp. 849--872.

.. [Black2015] Black, Paul E. (26 January 2015). "Reservoir sampling". Dictionary of Algorithms and Data Structures.

.. [Efraimidis2006] Efraimidis, Pavlos S.; Spirakis, Paul G. (2006-03-16). "Weighted random sampling with a reservoir".
    Information Processing Letters. 97 (5): 181–185.

.. [Vitter1985] Vitter, Jeffrey S. "Random sampling with a reservoir." ACM Transactions on Mathematical Software (TOMS) 11.1 (1985): 37-57.
