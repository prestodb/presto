====================================
Mathematical Functions
====================================

.. spark:function:: abs(x) -> [same as x]

    Returns the absolute value of ``x``.

.. spark:function:: acosh(x) -> double

    Returns inverse hyperbolic cosine of ``x``.

.. spark:function:: asinh(x) -> double

    Returns inverse hyperbolic sine of ``x``.

.. spark:function:: atanh(x) -> double

    Returns inverse hyperbolic tangent of ``x``.

.. spark:function:: add(x, y) -> [same as x]

    Returns the result of adding x to y. The types of x and y must be the same.
    For integral types, overflow results in an error. Corresponds to sparks's operator ``+``.

.. spark:function:: ceil(x) -> [same as x]

    Returns ``x`` rounded up to the nearest integer.  
    Supported types are: BIGINT and DOUBLE.

.. spark:function:: csc(x) -> double

    Returns the cosecant of ``x``.

.. spark:function:: divide(x, y) -> double

    Returns the results of dividing x by y. Performs floating point division.
    Corresponds to Spark's operator ``/``. ::

        SELECT 3 / 2; -- 1.5
        SELECT 2L / 2L; -- 1.0
        SELECT 3 / 0; -- NULL

.. spark:function:: exp(x) -> double

    Returns Euler's number raised to the power of ``x``.

.. spark:function:: floor(x) -> [same as x]

    Returns ``x`` rounded down to the nearest integer.
    Supported types are: BIGINT and DOUBLE.

.. spark:function:: multiply(x, y) -> [same as x]

    Returns the result of multiplying x by y. The types of x and y must be the same.
    For integral types, overflow results in an error. Corresponds to Spark's operator ``*``.

.. spark:function:: not(x) -> boolean

    Logical not. ::

        SELECT not true; -- false
        SELECT not false; -- true
        SELECT not NULL; -- NULL

.. spark:function:: pmod(n, m) -> [same as n]

    Returns the positive remainder of n divided by m.

.. spark:function:: power(x, p) -> double

    Returns ``x`` raised to the power of ``p``.

.. spark:function:: rand() -> double

    Returns a random value with independent and identically distributed uniformly distributed values in [0, 1). ::

        SELECT rand(); -- 0.9629742951434543
        SELECT rand(0); -- 0.7604953758285915
        SELECT rand(null); -- 0.7604953758285915

.. spark:function:: remainder(n, m) -> [same as n]

    Returns the modulus (remainder) of ``n`` divided by ``m``. Corresponds to Spark's operator ``%``.

.. spark:function:: round(x, d) -> [same as x]

    Returns ``x`` rounded to ``d`` decimal places using HALF_UP rounding mode. 
    In HALF_UP rounding, the digit 5 is rounded up.

.. spark:function:: sec(x) -> double

    Returns the secant of ``x``.

.. spark:function:: subtract(x, y) -> [same as x]

    Returns the result of subtracting y from x. The types of x and y must be the same.
    For integral types, overflow results in an error. Corresponds to Spark's operator ``-``.

.. spark:function:: unaryminus(x) -> [same as x]

    Returns the negative of `x`.  Corresponds to Spark's operator ``-``.
