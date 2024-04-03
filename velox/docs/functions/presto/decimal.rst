=================
Decimal Operators
=================

DECIMAL type is designed to represent floating point numbers precisely.
Mathematical operations on decimal values are exact, except for division. On
the other hand, DOUBLE and REAL types are designed to represent floating point
numbers approximately. Mathematical operations on double and real values are
approximate.

For example, the number 5,000,000,000,000,000 can be represented using DOUBLE.
However, the number 5,000,000,000,000,000.15 cannot be represented using
DOUBLE, but it can be represented using DECIMAL. See
https://en.wikipedia.org/wiki/Double-precision_floating-point_format for more
details.

DECIMAL type has 2 parameters: precision and scale. Precision is the total
number of digits used to represent the number. Scale is the number of digits
after the decimal point. Naturally, scale must not exceed precision. In
addition, precision cannot exceed 38.

::

	decimal(p, s)

	p >= 1 && p <= 38
	s >= 0 && s <= p

The number 5,000,000,000,000,000.15 can be represented using DECIMAL(18, 2).
This number needs at least 18 digits (precision) of which at least 2 digits
must appear after the decimal point (scale). This number can be represented
using any DECIMAL type where scale >= 2 and precision is >= scale + 16.

Addition and Subtraction
------------------------

To represent the results of adding two decimal numbers we need to use max
(s1, s2) digits after the decimal point and max(p1 - s1, p2 - s2) + 1 digits
before the decimal point.

::

	p = max(p1 - s1, p2 - s2) + 1 + max(s1, s2)
	s = max(s1, s2)

It is easiest to understand this formula by thinking about column addition where
we place two numbers one under the other and line up decimal points.

::

        1.001
     9999.5
   -----------
    10000.501

We can see that the result needs max(s1, s2) digits after the decimal point and
max(p1 - s1, p2 - s2) + 1 digits before the decimal point.

The precision of the result may exceed 38. There are two options. One option is
to say that addition and subtraction is supported as long as p <= 38 and reject
operations that produce p > 38. Another option is to cap p at 38 and allow the
operation to succeed as long as the actual result can be represented using 38
digits. In this case, users experience runtime errors when the actual result
cannot be represented using 38 digits. Presto implements the second option. Velox
implementation matches Presto.

Multiplication
--------------

To represent the results of multiplying two decimal numbers we need s1 + s2
digits after the decimal point and p1 + p2 digits overall.

::

	p = p1 + p2
	s = s1 + s2

To multiply two numbers we can multiply them as integers ignoring the decimal
points, then add up the number of digits after the decimal point in the
original numbers and place the decimal point that many digits away in the
result.

To multiply 0.01 with 0.001, we can multiply 1 with 1, then place the decimal
point 5 digits to the left: 0.00001. Hence, the scale of the result is the sum
of scales of the inputs.

When multiplying two integers with p1 and p2 digits respectively we get a result
that is strictly less than 10^p1 * 10^p2 = 10^(p1 + p2). Hence, we need at most
p1 + p2 digits to represent the result.

Both scale and precision of the result may exceed 38. There are two options
again. One option is to say that multiplication is supported as long as p <=
38 (by definition, s does not exceed p and therefore does not exceed 38 if p <=
38). Another option is to cap p and s at 38 and allow operation to succeed as
long as the actual result can be represented as a decimal(38, s). In this case,
users experience runtime errors when the actual result cannot be represented as a
decimal(38, s). Presto implements a third option. Reject the operation if s
exceeds 38 and cap p at 38 when s <= 38. In this case some operations are rejected
outright while others are allowed to proceed, but may produce runtime errors. Velox
implementation matches Presto.

Division
--------

Perfect division is not possible. For example, 1 / 3 cannot be represented as a
decimal value.

When dividing a number with p1 digits over a number of s2 scale, we need p1 + s2
digits for the result. In the worst case we divide by 0.0000001, which
effectively is a multiplication by 10^s2.

Presto also chooses to extend the scale of the result to a maximum of scales of
the inputs. It is not clear exactly why.

::

	s = max(s1, s2)

To support increased scale, the result precision needs to be extended by the
difference in s1 and s.

::

	p = p1 + s2 + max(0, s2 - s1)

Like in Addition, the precision of the result may exceed 38. The choices are the
same. Presto chooses to cap p at 38 and allow runtime errors.

The choice of formula for the result scale produces the following behavior that
might be surprising to users.

::

    0.01 / 0.001 = 0.000 (not 0.00001)

, where 0.01 is a decimal(3, 2) and 0.001 is a decimal (4, 3).

Velox implementation matches Presto.

Decimal Functions
-----------------

.. function:: abs(x: decimal(p, s)) -> r: decimal(p, s)

    Returns absolute value of x (r = `|x|`).

.. function:: negate(x: decimal(p, s)) -> r: decimal(p, s)

    Returns negated value of x (r = -x).

.. function:: plus(x: decimal(p1, s1), y: decimal(p2, s2)) -> r: decimal(p, s)

    Returns the result of adding x to y (r = x + y).

    x and y are decimal values with possibly different precisions and scales. The
    precision and scale of the result are calculated as follows:
    ::

        p = min(38, max(p1 - s1, p2 - s2) + 1 + max(s1, s2))
        s = max(s1, s2)

    Throws if result cannot be represented using precision calculated above.

.. function:: minus(x: decimal(p1, s1), y: decimal(p2, s2)) -> r: decimal(p, s)

    Returns the result of subtracting y from x (r = x - y).

    x and y are decimal values with possibly different precisions and scales. The
    precision and scale of the result are calculated as follows:
    ::

        p = min(38, max(p1 - s1, p2 - s2) + 1 + max(s1, s2))
        s = max(s1, s2)

    Throws if result cannot be represented using precision calculated above.

.. function:: multiply(x: decimal(p1, s1), y: decimal(p2, s2)) -> r: decimal(p, s)

    Returns the result of multiplying x by y (r = x * y).

    x and y are decimal values with possibly different precisions and scales. The
    precision and scale of the result are calculated as follows:
    ::

        p = min(38, p1 + p2)
        s = s1 + s2

    The operation is not supported if s1 + s2 exceeds 38.

    Throws if result cannot be represented using precision calculated above.

.. function:: divide(x: decimal(p1, s1), y: decimal(p2, s2)) -> r: decimal(p, s)

    Returns the result of dividing x by y (r = x / y).

    x and y are decimal values with possibly different precisions and scales. The
    precision and scale of the result are calculated as follows:
    ::

        p = min(38, p1 + s2 + max(0, s2 - s1))
        s = max(s1, s2)

    Throws if y is zero or result cannot be represented using precision calculated
    above.
