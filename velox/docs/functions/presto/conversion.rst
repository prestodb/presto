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
   :widths: 25 25 25 25 25 25 25 25 25 25 25 25 25
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
     - timestamp with time zone
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
     - Y
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
     - Y
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
     - Y
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
     -
     - Y
     - Y
   * - timestamp
     -
     -
     -
     -
     -
     -
     -
     - Y
     - Y
     - Y
     - Y
     -
   * - timestamp with time zone
     -
     -
     -
     -
     -
     -
     -
     -
     - Y
     - Y
     -
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
     -
   * - decimal
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
integral number within the range of the result type. By default, casting from
strings that represent floating-point numbers is not allowed.
Casting from invalid input values throws.

Valid examples

::

  SELECT cast('12345' as bigint); -- 12345
  SELECT cast('+1' as tinyint); -- 1
  SELECT cast('-1' as tinyint); -- -1

Invalid examples

::

  SELECT cast('12345.67' as tinyint); -- Invalid argument
  SELECT cast('12345.67' as bigint); -- Invalid argument
  SELECT cast('1.2' as tinyint); -- Invalid argument
  SELECT cast('-1.8' as tinyint); -- Invalid argument
  SELECT cast('1.' as tinyint); -- Invalid argument
  SELECT cast('-1.' as tinyint); -- Invalid argument
  SELECT cast('0.' as tinyint); -- Invalid argument
  SELECT cast('.' as tinyint); -- Invalid argument
  SELECT cast('-.' as tinyint); -- Invalid argument

From decimal
^^^^^^^^^^^^

The decimal part is rounded.

Valid examples

::

  SELECT cast(2.56 decimal(6, 2) as integer); -- 3
  SELECT cast(3.46 decimal(6, 2) as integer); -- 3

Invalid examples

::
  
  SELECT cast(214748364890 decimal(12, 2) as integer); -- Out of range

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
  SELECT cast(0.5 as boolean); -- true
  SELECT cast(-0.5 as boolean); -- true

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
  SELECT cast('Infinity' as real); -- Infinity (case insensitive)
  SELECT cast('-Infinity' as real); -- -Infinity (case insensitive)
  SELECT cast('NaN' as real); -- NaN (case insensitive)

Invalid examples

::

  SELECT cast('1.2a' as real); -- Invalid argument
  SELECT cast('1.2.3' as real); -- Invalid argument

There are a few corner cases where Velox behaves differently from Presto.
Presto throws INVALID_CAST_ARGUMENT on these queries, while Velox allows these
conversions. We keep the Velox behaivor by intention because it is more
consistent with other supported cases of cast.

::

  SELECT cast('infinity' as real); -- Infinity
  SELECT cast('-infinity' as real); -- -Infinity
  SELECT cast('inf' as real); -- Infinity
  SELECT cast('InfiNiTy' as real); -- Infinity
  SELECT cast('INFINITY' as real); -- Infinity
  SELECT cast('nAn' as real); -- NaN
  SELECT cast('nan' as real); -- NaN

Below cases are supported in Presto, but throw in Velox.

::

  SELECT cast('1.2f' as real); -- 1.2
  SELECT cast('1.2f' as double); -- 1.2
  SELECT cast('1.2d' as real); -- 1.2
  SELECT cast('1.2d' as double); -- 1.2

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

Casting from scalar types to string is allowed.

Valid examples

::

  SELECT cast(123 as varchar); -- '123'
  SELECT cast(123.45 as varchar); -- '123.45'
  SELECT cast(123.0 as varchar); -- '123.0'
  SELECT cast(nan() as varchar); -- 'NaN'
  SELECT cast(infinity() as varchar); -- 'Infinity'
  SELECT cast(true as varchar); -- 'true'
  SELECT cast(timestamp '1970-01-01 00:00:00' as varchar); -- '1970-01-01 00:00:00.000'
  SELECT cast(cast(22.51 as DECIMAL(5, 3)) as varchar); -- '22.510'
  SELECT cast(cast(-22.51 as DECIMAL(4, 2)) as varchar); -- '-22.51'
  SELECT cast(cast(0.123 as DECIMAL(3, 3)) as varchar); -- '0.123'
  SELECT cast(cast(1 as DECIMAL(6, 2)) as varchar); -- '1.00'
  SELECT cast(cast(0 as DECIMAL(6, 2)) as varchar); -- '0.00'

From Floating-Point Types
^^^^^^^^^^^^^^^^^^^^^^^^^
By default, casting a real or double to string returns standard notation if the magnitude of input value is greater than
or equal to 10 :superscript:`-3` but less than 10 :superscript:`7`, and returns scientific notation otherwise.

Positive zero returns '0.0' and negative zero returns '-0.0'. Positive infinity returns 'Infinity' and negative infinity
returns '-Infinity'. Positive and negative NaN returns 'NaN'.

If legacy_cast configuration property is true, the result is standard notation for all input value.

Valid examples if legacy_cast = false,

::

  SELECT cast(double '123456789.01234567' as varchar); -- '1.2345678901234567E8'
  SELECT cast(double '10000000.0' as varchar); -- '1.0E7'
  SELECT cast(double '12345.0' as varchar); -- '12345.0'
  SELECT cast(double '-0.001' as varchar); -- '-0.001'
  SELECT cast(double '-0.00012' as varchar); -- '-1.2E-4'
  SELECT cast(double '0.0' as varchar); -- '0.0'
  SELECT cast(double '-0.0' as varchar); -- '-0.0'
  SELECT cast(infinity() as varchar); -- 'Infinity'
  SELECT cast(-infinity() as varchar); -- '-Infinity'
  SELECT cast(nan() as varchar); -- 'NaN'
  SELECT cast(-nan() as varchar); -- 'NaN'

  SELECT cast(real '123456780.0' as varchar); -- '1.2345678E8'
  SELECT cast(real '10000000.0' as varchar); -- '1.0E7'
  SELECT cast(real '12345.0' as varchar); -- '12345.0'
  SELECT cast(real '-0.001' as varchar); -- '-0.001'
  SELECT cast(real '-0.00012' as varchar); -- '-1.2E-4'
  SELECT cast(real '0.0' as varchar); -- '0.0'
  SELECT cast(real '-0.0' as varchar); -- '-0.0'

Valid examples if legacy_cast = true,

::

  SELECT cast(double '123456789.01234567' as varchar); -- '123456789.01234567'
  SELECT cast(double '10000000.0' as varchar); -- '10000000.0'
  SELECT cast(double '-0.001' as varchar); -- '-0.001'
  SELECT cast(double '-0.00012' as varchar); -- '-0.00012'

  SELECT cast(real '123456780.0' as varchar); -- '123456784.0'
  SELECT cast(real '10000000.0' as varchar); -- '10000000.0'
  SELECT cast(real '12345.0' as varchar); -- '12345.0'
  SELECT cast(real '-0.00012' as varchar); -- '-0.00011999999696854502'


From TIMESTAMP
^^^^^^^^^^^^^^

By default, casting a timestamp to a string returns ISO 8601 format with space as separator
between date and time, and the year part is padded with zeros to 4 characters.

If legacy_cast configuration property is true, the result string uses character 'T'
as separator between date and time and the year part is not padded.

Valid examples if legacy_cast = false,

::

  SELECT cast(timestamp '1970-01-01 00:00:00' as varchar); -- '1970-01-01 00:00:00.000'
  SELECT cast(timestamp '2000-01-01 12:21:56.129' as varchar); -- '2000-01-01 12:21:56.129'
  SELECT cast(timestamp '384-01-01 08:00:00.000' as varchar); -- '0384-01-01 08:00:00.000'
  SELECT cast(timestamp '10000-02-01 16:00:00.000' as varchar); -- '10000-02-01 16:00:00.000'
  SELECT cast(timestamp '-10-02-01 10:00:00.000' as varchar); -- '-0010-02-01 10:00:00.000'

Valid examples if legacy_cast = true,

::

  SELECT cast(timestamp '1970-01-01 00:00:00' as varchar); -- '1970-01-01T00:00:00.000'
  SELECT cast(timestamp '2000-01-01 12:21:56.129' as varchar); -- '2000-01-01T12:21:56.129'
  SELECT cast(timestamp '384-01-01 08:00:00.000' as varchar); -- '384-01-01T08:00:00.000'
  SELECT cast(timestamp '-10-02-01 10:00:00.000' as varchar); -- '-10-02-01T10:00:00.000'

Cast to TIMESTAMP
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

From TIMESTAMP WITH TIME ZONE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The results depend on whether configuration property `adjust_timestamp_to_session_timezone` is set or not.

If set to true, input timezone is ignored and timestamp is returned as is. For example,
"1970-01-01 00:00:00.000 America/Los_Angeles" becomes "1970-01-01 08:00:00.000".

Otherwise, timestamp is shifted by the offset of the timezone. For example,
"1970-01-01 00:00:00.000 America/Los_Angeles" becomes "1970-01-01 00:00:00.000".

Valid examples

::

  -- `adjust_timestamp_to_session_timezone` is true
  SELECT to_unixtime(cast(timestamp '1970-01-01 00:00:00 America/Los_Angeles' as timestamp)); -- 28800.0 (1970-01-01 08:00:00.000)
  SELECT to_unixtime(cast(timestamp '2012-03-09 10:00:00 Asia/Chongqing' as timestamp)); -- 1.3312584E9 (2012-03-09 02:00:00.000)
  SELECT to_unixtime(cast(from_unixtime(0, '+06:00') as timestamp)); -- 0.0 (1970-01-01 00:00:00.000)
  SELECT to_unixtime(cast(from_unixtime(0, '-02:00') as timestamp)); -- 0.0 (1970-01-01 00:00:00.000)

  -- `adjust_timestamp_to_session_timezone` is false
  SELECT to_unixtime(cast(timestamp '1970-01-01 00:00:00 America/Los_Angeles' as timestamp)); -- 0.0 (1970-01-01 00:00:00.000)
  SELECT to_unixtime(cast(timestamp '2012-03-09 10:00:00 Asia/Chongqing' as timestamp)); -- 1.3312872E9 (2012-03-09 10:00:00.000)
  SELECT to_unixtime(cast(from_unixtime(0, '+06:00') as timestamp)); -- 21600.0 (1970-01-01 06:00:00.000)
  SELECT to_unixtime(cast(from_unixtime(0, '-02:00') as timestamp)); -- -7200.0 (1969-12-31 22:00:00.000)

Cast to TIMESTAMP WITH TIME ZONE
--------------------------------

From TIMESTAMP
^^^^^^^^^^^^^^

The results depend on whether configuration property `adjust_timestamp_to_session_timezone` is set or not.

If set to true, the output is adjusted to be equivalent as the input timestamp in UTC
based on the user provided `session_timezone` (if any). For example, when user supplies
"America/Los_Angeles" "1970-01-01 00:00:00.000" becomes "1969-12-31 16:00:00.000 America/Los_Angeles".

Otherwise, the user provided `session_timezone` (if any) is simply appended to the input
timestamp. For example, "1970-01-01 00:00:00.000" becomes "1970-01-01 00:00:00.000 America/Los_Angeles".

Valid examples

::

  -- `adjust_timestamp_to_session_timezone` is true
  SELECT cast(timestamp '1970-01-01 00:00:00' as timestamp with time zone); -- 1969-12-31 16:00:00.000 America/Los_Angeles
  SELECT cast(timestamp '2012-03-09 10:00:00' as timestamp with time zone); -- 2012-03-09 02:00:00.000 America/Los_Angeles
  SELECT cast(from_unixtime(0) as timestamp with time zone); -- 1969-12-31 16:00:00.000 America/Los_Angeles

  -- `adjust_timestamp_to_session_timezone` is false
  SELECT cast(timestamp '1970-01-01 00:00:00' as timestamp with time zone); -- 1970-01-01 00:00:00.000 America/Los_Angeles
  SELECT cast(timestamp '2012-03-09 10:00:00' as timestamp with time zone); -- 2012-03-09 10:00:00.000 America/Los_Angeles
  SELECT cast(from_unixtime(0) as timestamp with time zone); -- 1970-01-01 00:00:00.000 America/Los_Angeles

Cast to Date
------------

From strings
^^^^^^^^^^^^

Only ISO 8601 strings are supported: `[+-]YYYY-MM-DD`. Casting from invalid input values throws.

Valid examples

::

  SELECT cast('1970-01-01' as date); -- 1970-01-01

Invalid examples

::

  SELECT cast('2012' as date); -- Invalid argument
  SELECT cast('2012-10' as date); -- Invalid argument
  SELECT cast('2012-10-23T123' as date); -- Invalid argument
  SELECT cast('2012-10-23 (BC)' as date); -- Invalid argument
  SELECT cast('2012-Oct-23' as date); -- Invalid argument
  SELECT cast('2012/10/23' as date); -- Invalid argument
  SELECT cast('2012.10.23' as date); -- Invalid argument
  SELECT cast('2012-10-23 ' as date); -- Invalid argument

From TIMESTAMP
^^^^^^^^^^^^^^

Casting from timestamp to date is allowed. If present, the part of `hh:mm:ss`
in the input is ignored.

Valid examples

::

  SELECT cast(timestamp '1970-01-01 00:00:00' as date); -- 1970-01-01
  SELECT cast(timestamp '1970-01-01 23:59:59' as date); -- 1970-01-01

Cast to Decimal
---------------

From boolean type
^^^^^^^^^^^^^^^^^

Casting a boolean number to decimal of given precision and scale is allowed.
True value is converted to 1 and false to 0.

Valid examples

::

  SELECT cast(true as decimal(4, 2)); -- decimal '1.00'
  SELECT cast(false as decimal(8, 2)); -- decimal '0'

From integral types
^^^^^^^^^^^^^^^^^^^

Casting an integral number to a decimal of given precision and scale is allowed
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

From floating-point types
^^^^^^^^^^^^^^^^^^^^^^^^^

Casting a floating-point number to a decimal of given precision and scale is allowed
if the input value can be represented by the precision and scale. When the given
scale is less than the number of decimal places, the floating-point value is rounded.
The conversion precision is up to 15 for double and 6 for real according to the
significant decimal digits precision they provide. Casting from NaN or infinite value
throws.

Valid example

::

  SELECT cast(0.12 as decimal(4, 4)); -- decimal '0.1200'
  SELECT cast(0.12 as decimal(4, 1)); -- decimal '0.1'
  SELECT cast(0.19 as decimal(4, 1)); -- decimal '0.2'
  SELECT cast(0.123456789123123 as decimal(38, 18)); -- decimal '0.123456789123123000'
  SELECT cast(real '0.123456' as decimal(38, 18)); -- decimal '0.123456000000000000'

Invalid example

::

  SELECT cast(123.12 as decimal(6, 4)); -- Out of range
  SELECT cast(99999.99 as decimal(6, 2)); -- Out of range

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

From varchar
^^^^^^^^^^^^

Casting varchar to a decimal of given precision and scale is allowed
if the input value can be represented by the precision and scale. When casting from
a larger scale to a smaller one, the fraction part is rounded. Casting from invalid input value throws.

Valid example

::

  SELECT cast('9999999999.99' as decimal(12, 2)); -- decimal '9999999999.99'
  SELECT cast('1.556' as decimal(12, 2)); -- decimal '1.56'
  SELECT cast('1.554' as decimal(12, 2)); -- decimal '1.55'
  SELECT cast('-1.554' as decimal(12, 2)); -- decimal '-1.55'
  SELECT cast('+09' as decimal(12, 2)); -- decimal '9.00'
  SELECT cast('9.' as decimal(12, 2)); -- decimal '9.00'
  SELECT cast('.9' as decimal(12, 2)); -- decimal '0.90'
  SELECT cast('3E+2' as decimal(12, 2)); -- decimal '300.00'
  SELECT cast('3E+00002' as decimal(12, 2)); -- decimal '300.00'
  SELECT cast('3e+2' as decimal(12, 2)); -- decimal '300.00'
  SELECT cast('31.423e+2' as decimal(12, 2)); -- decimal '3142.30'
  SELECT cast('1.2e-2' as decimal(12, 2)); -- decimal '0.01'
  SELECT cast('1.2e-5' as decimal(12, 2)); -- decimal '0.00'
  SELECT cast('0000.123' as decimal(12, 2)); -- decimal '0.12'
  SELECT cast('.123000000' as decimal(12, 2)); -- decimal '0.12'

Invalid example

::

  SELECT cast('1.23e67' as decimal(38, 0)); -- Value too large
  SELECT cast('0.0446a' as decimal(9, 1)); -- Value is not a number
  SELECT cast('' as decimal(9, 1)); -- Value is not a number
  SELECT cast('23e-5d' as decimal(9, 1)); -- Value is not a number
  SELECT cast('1.23 ' as decimal(38, 0)); -- Value is not a number
  SELECT cast(' -3E+2' as decimal(12, 2)); -- Value is not a number
  SELECT cast('-3E+2.1' as decimal(12, 2)); -- Value is not a number
  SELECT cast('3E+' as decimal(12, 2)); -- Value is not a number

Miscellaneous
-------------

.. function:: typeof(x) -> varchar

    Returns the name of the type of x::

        SELECT typeof(123); -- integer
        SELECT typeof(1.5); -- double
        SELECT typeof(array[1,2,3]); -- array(integer)
