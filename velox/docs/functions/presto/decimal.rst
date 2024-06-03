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

Note: This definition of precision and scale may appear counterintuitive. It is
not uncommon to think about the number of digits after the decimal point as
precision and the number of digits before the decimal point as scale.

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

When dividing a number with p1 digits over a number of s2 scale, the biggest result requires s2 extra digits before the
decimal point. To get the largest number we must divide by 0.0000001, which effectively is a multiplication by 10^s2.
Hence, precision of the result needs to be at least p1 + s2.

Presto also chooses to extend the scale of the result to a maximum of scales of
the inputs.

::

	s = max(s1, s2)

To support increased scale, the result precision needs to be extended by the
difference in s1 and s.

::

	p = p1 + s2 + max(0, s2 - s1)

Like in Addition, the precision of the result may exceed 38. The choices are the
same. Presto chooses to cap p at 38 and allow runtime errors.

Let’s say `a` is of type decimal(p1, s1) with unscaled value `A` and `b` is of
type decimal(p2, s2) with unscaled value B.

::

    a = A / 10^s1
    b = B / 10^s2

The result type precision and scale are:

::

	s = max(s1, s2)
	p = p1 + s2 + max(0, s2 - s1)

The result 'r' has 's' digits after the decimal point and unscaled value R. We
derive the value of R as follows:

::

   r = a / b = (A / 10^s1) / (B / 10^s2) = A * 10^(s2 - s1) / B
   r = R / 10^s
   R = r * 10^s = A * 10^(s + s2 - s1) / B

To compute R, first rescale A using the rescale factor :code:`(s + s2 - s1)`,
then divide by B and round to the nearest whole. This method works as long as
rescale factor does not exceed 38. If :code:`s + s2 - s1` exceeds 38, an error
is raised.

The formula for the scale of the result is a choice. Presto chose max(s1, s2).
Other systems made different choices.

It is not clear why Presto chose max(s1, s2). Perhaps, the thinking was to
assume that user's desired accuracy is the max of input scales. However, one
could also say that desired accuracy is the scale of the dividend. In SQL,
literal values get their types assigned by the actual number of digits after
the decimal point. Hence, in the following SQL 1.2 has scale 1 and 0.01 has
scale 2. One may argue that user's intention is to work with accuracy of 2
digits after the decimal point, hence, max(s1, s2).

::

    SELECT 1.2 / 0.01

Modulus
-------

For the modulus operation :code:`a % b`, when a and b are integers, the result
`r` is less than `b` and less than or equal to `a`. Hence the number of digits
needed to represent `r` is no more than the minimum of the number of digits
needed to represent `a` or `b`. We can extend this to decimal inputs `a` and
`b` by computing the modulus of their unscaled values. However, we should
first make sure that `a` and `b` have the same scale. This can be achieved by
scaling up the input with lesser scale by the difference in the inputs' scales,
so both `a` and `b` have scale s. Once `a` and `b` have the same scale, we
compute the modulus of their unscaled values, A and B. `r` has s digits after
the decimal point, and since `r` does not need any more digits than the
minimum number of digits needed to represent `a` or `b`, the result precision
needs to be increased by the smaller of the differences in the precision and
scale of either inputs. Hence the result type precision and scale are:

::

    s = max(s1, s2)
    p = min(p2 - s2, p1 - s1) + max(s1, s2)

To compute R, we first rescale A and B to 's':

::

    A = a * 10^s1
    B = b * 10^s2

    A' = a * 10^s
    B' = b * 10^s

Then we compute modulus of the rescaled values:

::

    R = A' % B' = r * 10^s

For example, say `a` = 12.3 and `b` = 1.21, `r` = :code:`a % b` is calculated
as follows:

::

    s = max(1, 2) = 2
    p = min(2, 1) + s = 3

    A = 12.3 * 10^1 = 123
    B = 1.21 * 10^2 = 121

    A' = 12.3 * 10^2 = 1230
    B' = 1.21 * 10^2 = 121

    R = 1230 % 121 = 20 = 0.20 * 100

Decimal Functions
-----------------

.. function:: abs(x: decimal(p, s)) -> r: decimal(p, s)

    Returns absolute value of x (r = `|x|`).

.. function:: divide(x: decimal(p1, s1), y: decimal(p2, s2)) -> r: decimal(p, s)

    Returns the result of dividing x by y (r = x / y).

    x and y are decimal values with possibly different precisions and scales. The
    precision and scale of the result are calculated as follows:
    ::

        p = min(38, p1 + s2 + max(0, s2 - s1))
        s = max(s1, s2)

    Throws if y is zero, or result cannot be represented using precision calculated
    above, or rescale factor `max(s1, s2) - s1 + s2` exceeds 38.

.. function:: floor(x: decimal(p, s)) -> r: decimal(pr, 0)

    Returns 'x' rounded down to the nearest integer. The scale of the result is 0.
    The precision is calculated as:
    ::

        pr = min(38, p - s + min(s, 1))

.. function:: minus(x: decimal(p1, s1), y: decimal(p2, s2)) -> r: decimal(p, s)

    Returns the result of subtracting y from x (r = x - y).

    x and y are decimal values with possibly different precisions and scales. The
    precision and scale of the result are calculated as follows:
    ::

        p = min(38, max(p1 - s1, p2 - s2) + 1 + max(s1, s2))
        s = max(s1, s2)

    Throws if result cannot be represented using precision calculated above.

.. function:: modulus(x: decimal(p1, s1), y: decimal(p2, s2)) -> r: decimal(p, s)

    Returns the remainder from division of x by y (r = x % y).

    x and y are decimal values with possibly different precisions and scales. The
    precision and scale of the result are calculated as follows:
    ::

        p = min(p2 - s2, p1 - s1) + max(s1, s2)
        s = max(s1, s2)

    Throws if y is zero.

.. function:: multiply(x: decimal(p1, s1), y: decimal(p2, s2)) -> r: decimal(p, s)

    Returns the result of multiplying x by y (r = x * y).

    x and y are decimal values with possibly different precisions and scales. The
    precision and scale of the result are calculated as follows:
    ::

        p = min(38, p1 + p2)
        s = s1 + s2

    The operation is not supported if s1 + s2 exceeds 38.

    Throws if result cannot be represented using precision calculated above.

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

.. function:: round(x: decimal(p, s)) -> r: decimal(rp, 0)

    Returns 'x' rounded to the nearest integer. The scale of the result is 0.
    The precision is calculated as:
    ::

        pr = min(38, p - s + min(s, 1))

.. function:: round(x: decimal(p, s), d: integer) -> r: decimal(rp, s)

    Returns 'x' rounded to 'd' decimal places. The scale of the result is
    the same as the scale of the input. The precision is calculated as:
    ::

        p = min(38, p + 1)

    'd' can be positive, zero or negative. Returns 'x' unmodified if 'd' exceeds
    the scale of the input.

    ::

        SELECT round(123.45, 0); -- 123.00
        SELECT round(123.45, 1); -- 123.50
        SELECT round(123.45, 2); -- 123.45
        SELECT round(123.45, 3); -- 123.45
        SELECT round(123.45, -1); -- 120.00
        SELECT round(123.45, -2); -- 100.00
        SELECT round(123.45, -10); -- 0.00
