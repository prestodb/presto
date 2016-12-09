================================
Lambda Expressions and Functions
================================

Lambda Expression
-----------------

Lambda expressions are written with ``->``::

    x -> x + 1
    (x, y) -> x + y
    x -> regexp_like(x, 'a+')
    x -> x[1] / x[2]
    x -> IF(x > 0, x, -x)
    x -> COALESCE(x, 0)
    x -> CAST(x AS JSON)

Most SQL expressions can be used in a lambda body, with a few exceptions:

* Subqueries are not supported.
* The ``TRY`` function is not supported yet. (:func:`try_cast` is supported.)
* Capture is not supported yet:

  * Columns or relations cannot be referenced.
  * Only lambda variables from the inner-most lambda expression can be referenced.

Lambda Functions
----------------

.. function:: filter(array ARRAY<T>, function FUNCTION<T,BOOLEAN>) -> ARRAY<T>

    Constructs an array from those elements of ``array`` for which ``function`` returns true::

        SELECT filter(ARRAY [], x -> true); -- []
        SELECT filter(ARRAY [5, -6, NULL, 7], x -> x > 0); -- [5, 7]
        SELECT filter(ARRAY [5, NULL, 7, NULL], x -> x IS NOT NULL); -- [5, 7]

.. function:: map_filter(map MAP<K,V>, function FUNCTION<K,V,BOOLEAN>) -> MAP<K,V>

    Constructs a map from those entries of ``map`` for which ``function`` returns true::

        SELECT map_filter(MAP(ARRAY[], ARRAY[]), (k, v) -> true); -- {}
        SELECT map_filter(MAP(ARRAY[10, 20, 30], ARRAY['a', NULL, 'c']), (k, v) -> v IS NOT NULL); -- {10 -> a, 30 -> c}
        SELECT map_filter(MAP(ARRAY['k1', 'k2', 'k3'], ARRAY[20, 3, 15]), (k, v) -> v > 10); -- {k1 -> 20, k3 -> 15}

.. function:: transform(array ARRAY<T>, function FUNCTION<T,U>) -> ARRAY<U>

    Returns an array that applies ``function`` to each element of ``array``::

        SELECT transform(ARRAY [], x -> x + 1); -- []
        SELECT transform(ARRAY [5, 6], x -> x + 1); -- [6, 7]
        SELECT transform(ARRAY [5, NULL, 6], x -> COALESCE(x, 0) + 1); -- [6, 1, 7]
        SELECT transform(ARRAY ['x', 'abc', 'z'], x -> x || '0'); -- ['x0', 'abc0', 'z0']
        SELECT transform(ARRAY [ARRAY [1, NULL, 2], ARRAY[3, NULL]], a -> filter(a, x -> x IS NOT NULL)); -- [[1, 2], [3]]

.. function:: reduce(array ARRAY<T>, initialState S, inputFunction FUNCTION<S,T,S>, outputFunction FUNCTION<S,R>) -> R

    Returns a single value reduced from ``array``. ``inputFunction`` will
    be invoked for each element in ``array`` in order. In addition to taking
    the element, ``inputFunction`` takes the current state, initially
    ``initialState``, and returns the new state. ``outputFunction`` will be
    invoked to turn the final state into the result value. It may be identity
    function (``i -> i``). For example::

        SELECT reduce(ARRAY [], 0, (s, x) -> s + x, s -> s); -- 0
        SELECT reduce(ARRAY [5, 20, 50], 0, (s, x) -> s + x, s -> s); -- 75
        SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> s + x, s -> s); -- NULL
        SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> s + COALESCE(x, 0), s -> s); -- 75
        SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s); -- 75
        SELECT reduce(ARRAY [2147483647, 1], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); -- 2147483648
        SELECT reduce(ARRAY [5, 6, 10, 20], -- calculates arithmetic average: 10.25
                      CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
                      (s, x) -> CAST(ROW(x + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
                      s -> IF(s.count = 0, NULL, s.sum / s.count));
