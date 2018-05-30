==================
Lambda Expressions
==================

Lambda expressions are written with ``->``::

    x -> x + 1
    (x, y) -> x + y
    x -> regexp_like(x, 'a+')
    x -> x[1] / x[2]
    x -> IF(x > 0, x, -x)
    x -> COALESCE(x, 0)
    x -> CAST(x AS JSON)
    x -> x + TRY(1 / 0)

Most SQL expressions can be used in a lambda body, with a few exceptions:

* Subqueries are not supported. ``x -> 2 + (SELECT 3)``
* Aggregations are not supported. ``x -> max(y)``
