====================
Conversion Functions
====================

During expression evaluations, Velox doesn't implicitly convert expression
arguments to the correct types. If such a conversion is necessary, two provided
conversion functions can be used explicitly cast values to a particular type.

Conversion Functions
--------------------

.. function:: cast(value AS type) -> type

    Explicitly cast a value as a type. This can be used to cast a varchar to a
    numeric value type and vice versa.

.. function:: try_cast(value AS type) -> type

    Like :func:`cast`, but returns null if the cast fails. ``try_cast(x AS type)``
    is different from ``try(cast(x AS type))`` in that ``try_cast`` only suppresses
    errors happening during the casting itself but not those during the evaluation
    of its argument. For example, ``try_cast(x / 0 as double)`` throws a divide-by-0
    error, while ``try(cast(x / 0 as double))`` returns a NULL.

Supported Conversions
---------------------

The supported conversions are listed below, with from-types given at rows and to-types given at columns. Conversions of ARRAY, MAP, and ROW types
are supported if the conversion of their element types are supported. In addition,
supported conversions to/from JSON are listed in :doc:`json`.

.. list-table::
   :widths: 25 25 25 25 25 25 25 25 25 25 25 25
   :header-rows: 1

   * -
     - tinyint
     - smallint
     - integer
     - bigint
     - boolean
     - real
     - double
     - varchar
     - timestamp
     - date
     - decimal
   * - tinyint
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     -
     -
     - Y
   * - smallint
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     -
     -
     - Y
   * - integer
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     -
     -
     - Y
   * - bigint
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     -
     -
     - Y
   * - boolean
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     -
     -
     -
   * - real
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     -
     -
     -
   * - double
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     -
     -
     -
   * - varchar
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y
     -
   * - timestamp
     -
     -
     -
     -
     -
     -
     -
     - Y
     -
     - Y
     -
   * - date
     -
     -
     -
     -
     -
     -
     -
     -
     - Y
     -
     -
   * - decimal
     - Y
     - Y
     - Y
     - Y
     -
     - Y
     - Y
     -
     -
     -
     - Y

Cast to Integral Types
----------------------

Integral types include bigint, integer, smallint, and tinyint.

From integral types
^^^^^^^^^^^^^^^^^^^

Casting one integral type to another is allowed when the input value is within
the range of the result type. Casting from invalid input values throws.

Valid examples:

::

  SELECT cast(1234567 as bigint); -- 1234567
  SELECT cast(12 as tinyint); -- 12

Invalid examples:

::

  SELECT cast(1234 as tinyint); -- Out of range
  SELECT cast(1234567 as smallint); -- Out of range

From floating-point types
^^^^^^^^^^^^^^^^^^^^^^^^^

Casting from floating-point input to an integral type rounds the input value to
the closest integral value. It is allowed when the rounded result is within the
range of the result type. Casting from invalid input values throws.

Valid examples

::

  SELECT cast(12345.12 as bigint); -- 12345
  SELECT cast(12345.67 as bigint); -- 12346
  SELECT cast(127.1 as tinyint); -- 127
  SELECT cast(nan() as integer); -- 0
  SELECT cast(nan() as smallint); -- 0
  SELECT cast(nan() as tinyint); -- 0

Invalid examples

::

  SELECT cast(127.8 as tinyint); -- Out of range
  SELECT cast(1234567.89 as smallint); -- Out of range
  SELECT cast(infinity() as bigint); -- Out of range

Casting NaN to bigint returns 0 in Velox but throws in Presto. We keep the
behavior of Velox by intention because this is more consistent with other
supported cases.

::

  SELECT cast(nan() as bigint); -- 0


From strings
^^^^^^^^^^^^

Casting a string to an integral type is allowed if the string represents an
integral number within the range of the result type. Casting from strings that
represent floating-point numbers is not allowed. Casting from invalid input
values throws.

Valid examples

::

  SELECT cast('12345' as bigint); -- 12345
  SELECT cast('+1' as tinyint); -- 1
  SELECT cast('-1' as tinyint); -- -1

Invalid examples

::

  SELECT cast('1234567' as tinyint); -- Out of range
  SELECT cast('12345.67' as tinyint); -- Invalid argument
  SELECT cast('1.2' as tinyint); -- Invalid argument
  SELECT cast('1a' as tinyint); -- Invalid argument
  SELECT cast('' as tinyint); -- Invalid argument
  SELECT cast('1,234,567' as bigint); -- Invalid argument
  SELECT cast('1'234'567' as bigint); -- Invalid argument
  SELECT cast('nan' as bigint); -- Invalid argument
  SELECT cast('infinity' as bigint); -- Invalid argument

Cast to Boolean
---------------

From integral and floating-point types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Casting from integral or floating-point numbers to boolean is allowed. Non-zero
numbers are converted to `true` while zero is converted to `false`.

Valid examples

::

  SELECT cast(1 as boolean); -- true
  SELECT cast(0 as boolean); -- false
  SELECT cast(12 as boolean); -- true
  SELECT cast(-1 as boolean); -- true
  SELECT cast(1.0 as boolean); -- true
  SELECT cast(1.1 as boolean); -- true
  SELECT cast(-1.1 as boolean); -- true
  SELECT cast(nan() as boolean); -- true
  SELECT cast(infinity() as boolean); -- true
  SELECT cast(0.0000000000001 as boolean); -- true

From strings
^^^^^^^^^^^^

There is a set of strings allowed to be casted to boolean. Casting from other strings to boolean throws.

Valid examples

::

  SELECT cast('1' as boolean); -- true
  SELECT cast('0' as boolean); -- false
  SELECT cast('t' as boolean); -- true (case insensitive)
  SELECT cast('true' as boolean); -- true (case insensitive)
  SELECT cast('f' as boolean); -- false (case insensitive)
  SELECT cast('false' as boolean); -- false (case insensitive)

Invalid examples

::

  SELECT cast('1.7E308' as boolean); -- Invalid argument
  SELECT cast('nan' as boolean); -- Invalid argument
  SELECT cast('infinity' as boolean); -- Invalid argument
  SELECT cast('12' as boolean); -- Invalid argument
  SELECT cast('-1' as boolean); -- Invalid argument
  SELECT cast('tr' as boolean); -- Invalid argument
  SELECT cast('tru' as boolean); -- Invalid argument

Cast to Floating-Point Types
----------------------------

From integral or floating-point types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Casting from an integral or floating-point number is allowed.

Valid examples

::

  SELECT cast(1 as real); -- 1.0
  SELECT cast(123.45 as real); -- 123.45

There are two cases where Velox behaves differently from Presto (:issue:`5934`) when casting
to real from a value beyond real's limit. We will fix them to follow Presto's
behavior.

::

  SELECT cast(1.7E308 as real); -- Presto returns Infinity but Velox throws
  SELECT cast(-1.7E308 as real); -- Presto returns -Infinity but Velox throws

From strings
^^^^^^^^^^^^

Casting a string to real is allowed if the string represents an integral or
floating-point number. Casting from invalid input values throws.

Valid examples

::

  SELECT cast('1.' as real); -- 1.0
  SELECT cast('1' as real); -- 1.0
  SELECT cast('1.7E308' as real); -- Infinity
  SELECT cast('infinity' as real); -- Infinity (case insensitive)
  SELECT cast('-infinity' as real); -- -Infinity (case insensitive)
  SELECT cast('nan' as real); -- NaN (case insensitive)

Invalid examples

::

  SELECT cast('1.7E308' as real); -- Out of range
  SELECT cast('1.2a' as real); -- Invalid argument
  SELECT cast('1.2.3' as real); -- Invalid argument

There are a few corner cases where Velox behaves differently from Presto.
Presto throws INVALID_CAST_ARGUMENT on these queries, while Velox allows these
conversions. We keep the Velox behaivor by intention because it is more
consistent with other supported cases of cast.

::

  SELECT cast('InfiNiTy' as real); -- Infinity
  SELECT cast('nAn' as real); -- NaN

From decimal
^^^^^^^^^^^^

Casting from decimal to double, float or any integral type is allowed. During decimal to an integral type conversion, if result overflows, or underflows, an exception is thrown.

Valid example

::

  SELECT cast(decimal '10.001' as double); -- 10.001

Invalid example

::

  SELECT cast(decimal '300.001' as tinyint); -- Out of range

Cast to String
--------------

Valid examples

::

  SELECT cast(123 as varchar); -- '123'
  SELECT cast(123.45 as varchar); -- '123.45'
  SELECT cast(123.0 as varchar); -- '123.0'
  SELECT cast(nan() as varchar); -- 'NaN'
  SELECT cast(infinity() as varchar); -- 'Infinity'
  SELECT cast(true as varchar); -- 'true'
  SELECT cast(timestamp '1970-01-01 00:00:00' as varchar); -- '1970-01-01T00:00:00.000'

Cast to Timestamp
-----------------

From strings
^^^^^^^^^^^^

Casting from a string to timestamp is allowed if the string represents a
timestamp in the format `YYYY-MM-DD` followed by an optional `hh:mm:ssZZ`.
Casting from invalid input values throws.

Valid examples

::

  SELECT cast('1970-01-01' as timestamp); -- 1970-01-01 00:00:00
  SELECT cast('1970-01-01 00:00:00' as timestamp); -- 1970-01-01 00:00:00
  SELECT cast('1970-01-01 02:01' as timestamp); -- 1970-01-01 02:01:00
  SELECT cast('1970-01-01 00:00:00-02:00' as timestamp); -- 1970-01-01 02:00:00

Invalid example

::

  SELECT cast('2012-Oct-23' as timestamp); -- Invalid argument

From date
^^^^^^^^^

Casting from date to timestamp is allowed.

Valid examples

::

  SELECT cast(date '1970-01-01' as timestamp); -- 1970-01-01 00:00:00
  SELECT cast(date '2012-03-09' as timestamp); -- 2012-03-09 00:00:00

Cast to Date
------------

From strings
^^^^^^^^^^^^

Casting from a string to date is allowed if the string represents a date in the
format `YYYY-MM-DD`. Casting from invalid input values throws.

Valid example

::

  SELECT cast('1970-01-01' as date); -- 1970-01-01

Invalid example

::

  SELECT cast('2012-Oct-23' as date); -- Invalid argument

From timestamp
^^^^^^^^^^^^^^

Casting from timestamp to date is allowed. If present, the part of `hh:mm:ss`
in the input is ignored.

Valid examples

::

  SELECT cast(timestamp '1970-01-01 00:00:00' as date); -- 1970-01-01
  SELECT cast(timestamp '1970-01-01 23:59:59' as date); -- 1970-01-01

Cast to Decimal
---------------

From integral types
^^^^^^^^^^^^^^^^^^^

Casting an integral numberto a decimal of given precision and scale is allowed
if the input value can be represented by the precision and scale. Casting from
invalid input values throws.

Valid examples

::

  SELECT cast(1 as decimal(4, 2)); -- decimal '1.00'
  SELECT cast(10 as decimal(4, 2)); -- decimal '10.00'
  SELECT cast(123 as decimal(5, 2)); -- decimal '123.00'

Invalid examples

::

  SELECT cast(123 as decimal(6, 4)); -- Out of range
  SELECT cast(123 as decimal(4, 2)); -- Out of range

From decimal
^^^^^^^^^^^^

Casting one decimal to another is allowed if the input value can be represented
by the result decimal type. When casting from a larger scale to a smaller one,
the fraction part is rounded.

Valid example

::

  SELECT cast(decimal '0.69' as decimal(4, 3)); -- decimal '0.690'
  SELECT cast(decimal '0.69' as decimal(4, 1)); -- decimal '0.7'

Invalid example

::

  SELECT cast(decimal '-1000.000' as decimal(6, 4)); -- Out of range
  SELECT cast(decimal '123456789' as decimal(9, 1)); -- Out of range
