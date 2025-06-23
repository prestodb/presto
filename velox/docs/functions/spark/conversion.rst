====================
Conversion Functions
====================

.. spark:function:: cast(value AS type) -> type

    Explicitly cast a ``value`` to a specified ``type``.
    Follows the behavior when Spark ANSI mode is disabled, and does not support
    the behavior when ANSI is turned on:

    * If the ``value`` exceeds the range of the ``type``, no error is raised.
      Instead, the ``value`` is "wrapped" around.

    * If the ``value`` has an invalid format or contains characters incompatible
      with the target ``type``, the cast function returns NULL. ::

        SELECT cast(128 as tinyint); -- -128
        SELECT cast('2012-Oct-23' as date); -- NULL

.. spark:function:: try_cast(value AS type) -> type

    Returns the ``value`` cast to ``type`` if possible, or NULL if not possible.
    Its behavior is independent of the ANSI mode setting, and it acts identically
    to cast with ANSI mode enabled but returns NULL rather than throwing errors
    for failure to cast.
    ``try_cast`` differs from ``cast`` function with ANSI mode disabled in following case:

    * If the ``value`` cannot fit within the domain of ``type``, the result is NULL. ::

        SELECT try_cast(128 as tinyint); -- NULL
        SELECT try_cast(cast(550000.0 as DECIMAL(8, 1)) as smallint); -- NULL
        SELECT try_cast(1e12 as int); -- NULL

Cast from UNKNOWN Type
----------------------

Casting from UNKNOWN type to all other scalar types is supported, e.g., cast(NULL as int).

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

  SELECT cast('1234567' as tinyint); -- NULL // Reason: Out of range
  SELECT cast('1a' as tinyint); -- NULL // Invalid argument
  SELECT cast('' as tinyint); -- NULL // Invalid argument
  SELECT cast('1,234,567' as bigint); -- NULL // Invalid argument
  SELECT cast('1'234'567' as bigint); -- NULL // Invalid argument
  SELECT cast('nan' as bigint); -- NULL // Invalid argument
  SELECT cast('infinity' as bigint); -- NULL // Invalid argument

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

From timestamp
^^^^^^^^^^^^^

Casting timestamp as integral types returns the number of seconds by converting timestamp as microseconds, dividing by the number of microseconds in a second, and then rounding down to the nearest second since the epoch (1970-01-01 00:00:00 UTC).

Valid examples

::

  SELECT cast(cast('1970-01-01 00:00:00' as timestamp) as bigint); -- 0
  SELECT cast(cast('1970-01-01 00:00:00' as timestamp) as smallint); -- 0
  SELECT cast(cast('1970-01-01 00:00:00' as timestamp) as tinyint); -- 0
  SELECT cast(cast('2000-01-01 12:21:56' as timestamp) as bigint); -- 946684916
  SELECT cast(cast('2025-02-25 08:00:26.88' as timestamp) as bigint); -- 1740470426
  SELECT cast(cast('2025-02-25 08:00:26.88' as timestamp) as integer); -- 1740470426
  SELECT cast(cast('2025-02-25 08:00:26.88' as timestamp) as smallint); -- 30874
  SELECT cast(cast('2025-02-25 08:00:26.88' as timestamp) as tinyint); -- -102

Cast to Boolean
---------------

From VARCHAR
^^^^^^^^^^^^

The strings `t, f, y, n, 1, 0, yes, no, true, false` and their upper case equivalents are allowed to be casted to boolean.
Casting from other strings to boolean throws.

Valid examples

::

  SELECT cast('1' as boolean); -- true
  SELECT cast('0' as boolean); -- false
  SELECT cast('t' as boolean); -- true (case insensitive)
  SELECT cast('true' as boolean); -- true (case insensitive)
  SELECT cast('f' as boolean); -- false (case insensitive)
  SELECT cast('false' as boolean); -- false (case insensitive)
  SELECT cast('y' as boolean); -- true (case insensitive)
  SELECT cast('yes' as boolean); -- true (case insensitive)
  SELECT cast('n' as boolean); -- false (case insensitive)
  SELECT cast('no' as boolean); -- false (case insensitive)

Invalid examples

::

  SELECT cast('1.7E308' as boolean); -- NULL // Invalid argument
  SELECT cast('nan' as boolean); -- NULL // Invalid argument
  SELECT cast('infinity' as boolean); -- NULL // Invalid argument
  SELECT cast('12' as boolean); -- NULL // Invalid argument
  SELECT cast('-1' as boolean); -- NULL // Invalid argument
  SELECT cast('tr' as boolean); -- NULL // Invalid argument
  SELECT cast('tru' as boolean); -- NULL // Invalid argument

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

  SELECT cast('2012-Oct-23' as date); -- NULL // Invalid argument
  SELECT cast('2012/10/23' as date); -- NULL // Invalid argument
  SELECT cast('2012.10.23' as date); -- NULL // Invalid argument

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

Cast to Varbinary
-----------------

From integral types
^^^^^^^^^^^^^^^^^^^

Casting integral value to varbinary type is allowed.
Bytes of input value are converted into an array of bytes in little-endian order.
Supported types are tinyint, smallint, integer and bigint.

Valid example

::

  SELECT cast(cast(18 as tinyint) as binary); -- [12]
  SELECT cast(cast(180 as smallint) as binary); -- [00 B4]
  SELECT cast(cast(180000 as integer) as binary); -- [00 02 BF 20]
  SELECT cast(cast(180000 as bigint) as binary); -- [00 00 00 00 00 02 BF 20]

Cast to Timestamp
-----------------

From integral types
^^^^^^^^^^^^^^^^^^^

Casting integral value to timestamp type is allowed.
The input value is treated as the number of seconds since the epoch (1970-01-01 00:00:00 UTC).
Supported types are tinyint, smallint, integer and bigint.

Valid example

::

  SELECT cast(0 as timestamp); -- 1970-01-01 00:00:00
  SELECT cast(1727181032 as timestamp); -- 2024-09-24 12:30:32
  SELECT cast(9223372036855 as timestamp); -- 294247-01-10 04:00:54.775807
  SELECT cast(-9223372036855 as timestamp); -- 290308-12-21 19:59:05.224192

From floating-point types
^^^^^^^^^^^^^^^^^^^^^^^^^

Casting from floating-point input to timestamp type is allowed.
The input value is treated as the number of seconds since the epoch (1970-01-01 00:00:00 UTC) and converted to microseconds by truncating the fractional part.

Valid examples

::

  SELECT cast(0.0 as timestamp); -- 1970-01-01 00:00:00
  SELECT cast(1727181032.0 as timestamp); -- 2024-09-24 12:30:32
  SELECT cast(-1727181032.0 as timestamp); -- 1915-04-09 11:29:28
  SELECT cast(cast(9223372036855.999 as double) as timestamp); -- 294247-01-10 04:00:54.775807
  SELECT cast(cast(-9223372036856.999 as double) as timestamp); -- -290308-12-21 19:59:05.224192
  SELECT cast(cast(1.79769e+308 as double) as timestamp); -- 294247-01-10 04:00:54.775807
  SELECT cast(cast('inf' as double) as timestamp); -- NULL
  SELECT cast(cast('nan' as double) as timestamp); -- NULL

From boolean
^^^^^^^^^^^^

Casting from boolean to timestamp is supported.

* ``true`` - cast to **1970-01-01 00:00:00.000001**
* ``false`` - cast to **1970-01-01 00:00:00** (epoch)

Valid examples

::

  SELECT cast(true as timestamp); -- 1970-01-01 00:00:00.000001
  SELECT cast(false as timestamp); -- 1970-01-01 00:00:00
