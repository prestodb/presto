====================
Conversion Functions
====================

Cast to Integral Types
----------------------

Integral types include bigint, integer, smallint, and tinyint.

From integral types
^^^^^^^^^^^^^^^^^^^

Casting one integral type to another is allowed. When the input value exceeds the range of result type,
a value of the result type is created forcedly with the input value.

Valid examples:

::

  SELECT cast(1234567 as bigint); -- 1234567
  SELECT cast(12 as tinyint); -- 12
  SELECT cast(1234 as tinyint); -- -46
  SELECT cast(1234567 as smallint); -- -10617

From floating-point types
^^^^^^^^^^^^^^^^^^^^^^^^^

Casting from floating-point input to an integral type truncates the input value.
It is allowed when the truncated result exceeds the range of result type.

Valid examples

::

  SELECT cast(12345.12 as bigint); -- 12345
  SELECT cast(12345.67 as bigint); -- 12345
  SELECT cast(127.1 as tinyint); -- 127
  SELECT cast(127.8 as tinyint); -- 127
  SELECT cast(1234567.89 as smallint); -- -10617
  SELECT cast(cast('inf' as double) as bigint); -- 9223372036854775807
  SELECT cast(cast('nan' as double) as integer); -- 0
  SELECT cast(cast('nan' as double) as smallint); -- 0
  SELECT cast(cast('nan' as double) as tinyint); -- 0
  SELECT cast(cast('nan' as double) as bigint); -- 0

From strings
^^^^^^^^^^^^

Casting a string to an integral type is allowed if the string represents a number within the range of result type.
Casting from strings that represent floating-point numbers truncates the decimal part of the input value.
Casting from invalid input values throws.

Valid examples

::

  SELECT cast('12345' as bigint); -- 12345
  SELECT cast('+1' as tinyint); -- 1
  SELECT cast('-1' as tinyint); -- -1
  SELECT cast('12345.67' as bigint); -- 12345
  SELECT cast('1.2' as tinyint); -- 1
  SELECT cast('-1.8' as tinyint); -- -1
  SELECT cast('+1' as tinyint); -- 1
  SELECT cast('1.' as tinyint); -- 1
  SELECT cast('-1' as tinyint); -- -1
  SELECT cast('-1.' as tinyint); -- -1
  SELECT cast('0.' as tinyint); -- 0
  SELECT cast('.' as tinyint); -- 0
  SELECT cast('-.' as tinyint); -- 0

Invalid examples

::

  SELECT cast('1234567' as tinyint); -- Out of range
  SELECT cast('1a' as tinyint); -- Invalid argument
  SELECT cast('' as tinyint); -- Invalid argument
  SELECT cast('1,234,567' as bigint); -- Invalid argument
  SELECT cast('1'234'567' as bigint); -- Invalid argument
  SELECT cast('nan' as bigint); -- Invalid argument
  SELECT cast('infinity' as bigint); -- Invalid argument

From decimal
^^^^^^^^^^^^

The decimal part will be truncated for casting to an integer.
It is allowed when the truncated result exceeds the range of result type.

Valid examples

::

  SELECT cast(cast(2.56 as DECIMAL(6, 2)) as bigint); -- 2
  SELECT cast(cast(3.46 as DECIMAL(6, 2)) as bigint); -- 3
  SELECT cast(cast(5500.0 as DECIMAL(5, 1)) as tinyint); -- 124
  SELECT cast(cast(2147483648.90 as DECIMAL(12, 2)) as tinyint); -- 0
  SELECT cast(cast(2147483648.90 as DECIMAL(12, 2)) as integer); -- -2147483648
  SELECT cast(cast(2147483648.90 as DECIMAL(12, 2)) as bigint); -- 2147483648

Cast to String
--------------

From TIMESTAMP
^^^^^^^^^^^^^^

Casting a timestamp to a string returns ISO 8601 format with space as separator between date and time,
and the year part is padded with zeros to 4 characters.
The conversion precision is microsecond, and trailing zeros are not appended.
When the year exceeds 9999, a positive sign is added.

Valid examples

::

  SELECT cast(cast('1970-01-01 00:00:00' as timestamp) as string); -- '1970-01-01 00:00:00'
  SELECT cast(cast('2000-01-01 12:21:56.129' as timestamp) as string); -- '2000-01-01 12:21:56.129'
  SELECT cast(cast('2000-01-01 12:21:56.100000' as timestamp) as string); -- '2000-01-01 12:21:56.1'
  SELECT cast(cast('2000-01-01 12:21:56.129900' as timestamp) as string); -- '2000-01-01 12:21:56.1299'
  SELECT cast(cast('10000-02-01 16:00:00.000' as timestamp) as string); -- '+10000-02-01 16:00:00'
  SELECT cast(cast('0384-01-01 08:00:00.000' as timestamp) as string); -- '0384-01-01 08:00:00'
  SELECT cast(cast('-0010-02-01 10:00:00.000' as timestamp) as string); -- '-0010-02-01 10:00:00'

Cast to Date
------------

From strings
^^^^^^^^^^^^

All Spark supported patterns are allowed:
  
  * ``[+-](YYYY-MM-DD)``
  * ``[+-]yyyy*``
  * ``[+-]yyyy*-[m]m``
  * ``[+-]yyyy*-[m]m-[d]d``
  * ``[+-]yyyy*-[m]m-[d]d *``
  * ``[+-]yyyy*-[m]m-[d]dT*``

The asterisk ``*`` in ``yyyy*`` stands for any numbers.
For the last two patterns, the trailing ``*`` can represent none or any sequence of characters, e.g:
  
  * "1970-01-01 123"
  * "1970-01-01 (BC)"
  
All leading and trailing UTF8 white-spaces will be trimmed before cast.
Casting from invalid input values throws.

Valid examples

::

  SELECT cast('1970' as date); -- 1970-01-01
  SELECT cast('1970-01' as date); -- 1970-01-01
  SELECT cast('1970-01-01' as date); -- 1970-01-01
  SELECT cast('1970-01-01T123' as date); -- 1970-01-01
  SELECT cast('1970-01-01 ' as date); -- 1970-01-01
  SELECT cast('1970-01-01 (BC)' as date); -- 1970-01-01

Invalid examples

::

  SELECT cast('2012-Oct-23' as date); -- Invalid argument
  SELECT cast('2012/10/23' as date); -- Invalid argument
  SELECT cast('2012.10.23' as date); -- Invalid argument

Cast to Decimal
---------------

From varchar
^^^^^^^^^^^^

Casting varchar to a decimal of given precision and scale is allowed.
The behavior is similar with Presto except Spark allows leading and trailing white-spaces in input varchars.

Valid example

::

  SELECT cast(' 1.23' as decimal(38, 0)); -- 1
  SELECT cast('1.23 ' as decimal(38, 0)); -- 1
  SELECT cast('  1.23  ' as decimal(38, 0)); -- 1
  SELECT cast(' -3E+2' as decimal(12, 2)); -- -300.00
  SELECT cast('-3E+2 ' as decimal(12, 2)); -- -300.00
  SELECT cast('  -3E+2  ' as decimal(12, 2)); -- -300.00
