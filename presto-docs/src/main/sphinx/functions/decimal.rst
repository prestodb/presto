===============================
Decimal Functions and Operators
===============================

Decimal Literals
----------------

    Use ``DECIMAL 'xxxxxxx.yyyyyyy'`` syntax to define literal of DECIMAL type.

    The precision of DECIMAL type for literal will be equal to number of digits
    in literal (including trailing and leading zeros).
    The scale will be equal to number of digits in fractional part (including trailing zeros).

=========================================== =============================
Example literal                             Data type
=========================================== =============================
``DECIMAL '0'``                               ``DECIMAL(1)``
``DECIMAL '12345'``                           ``DECIMAL(5)``
``DECIMAL '0000012345.1234500000'``           ``DECIMAL(20, 10)``
=========================================== =============================

Binary Arithmetic Decimal Operators
-----------------------------------

    Standard mathematical operators are supported. The table below explains
    precision and scale calculation rules for result.
    Assuming ``x`` is of type ``DECIMAL(xp, xs)`` and ``y`` is of type ``DECIMAL(yp, ys)``.

+---------------+-----------------------------------+-----------------------------------+
| Operation     | Result type precision             | Result type scale                 |
+---------------+-----------------------------------+-----------------------------------+
| ``x + y``     | ::                                |                                   |
|               |                                   |                                   |
| and           |   min(38,                         | ``max(xs, ys)``                   |
|               |       1 +                         |                                   |
| ``x - y``     |         min(xs, ys) +             |                                   |
|               |         min(xp - xs, yp - ys)     |                                   |
|               |      )                            |                                   |
+---------------+-----------------------------------+-----------------------------------+
| ``x * y``     | ``min(38, xp + yp)``              | ``xs + ys``                       |
+---------------+-----------------------------------+-----------------------------------+
| ``x / y``     | ::                                |                                   |
|               |                                   |                                   |
|               |   min(38,                         | ``max(xs, ys)``                   |
|               |       xp + ys                     |                                   |
|               |          + max(0, ys-xs)          |                                   |
|               |      )                            |                                   |
+---------------+-----------------------------------+-----------------------------------+
| ``x % y``     | ::                                |                                   |
|               |                                   |                                   |
|               |   min(xp - xs, yp - ys) +         | ``max(xs, ys)``                   |
|               |   max(xs, bs)                     |                                   |
+---------------+-----------------------------------+-----------------------------------+

    If the mathematical result of the operation is not exactly representable with
    the precision and scale of the result data type,
    then an exception condition is raised - ``Value is out of range``.

Comparison Operators
--------------------

    All standard comparison operators and ``BETWEEN`` operator work for ``DECIMAL`` type.

Unary Decimal Operators
-----------------------

    The ``-`` operator performs negation. The type of result is same as type of argument.
