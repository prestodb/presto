=====================================
Date and Time Functions and Operators
=====================================

Date and Time Operators
-----------------------

======== ===================================================== ===========================
Operator Example                                               Result
======== ===================================================== ===========================
``+``    ``date '2012-08-08' + interval '2' day``              ``2012-08-10``
``+``    ``time '01:00' + interval '3' hour``                  ``04:00:00.000``
``+``    ``timestamp '2012-08-08 01:00' + interval '29' hour`` ``2012-08-09 06:00:00.000``
``+``    ``timestamp '2012-10-31 01:00' + interval '1' month`` ``2012-11-30 01:00:00.000``
``+``    ``interval '2' day + interval '3' hour``              ``2 03:00:00.000``
``+``    ``interval '3' year + interval '5' month``            ``3-5``
``-``    ``date '2012-08-08' - interval '2' day``              ``2012-08-06``
``-``    ``time '01:00' - interval '3' hour``                  ``22:00:00.000``
``-``    ``timestamp '2012-08-08 01:00' - interval '29' hour`` ``2012-08-06 20:00:00.000``
``-``    ``timestamp '2012-10-31 01:00' - interval '1' month`` ``2012-09-30 01:00:00.000``
``-``    ``interval '2' day - interval '3' hour``              ``1 21:00:00.000``
``-``    ``interval '3' year - interval '5' month``            ``2-7``
======== ===================================================== ===========================

Time Zone Conversion
--------------------

The ``AT TIME ZONE`` operator sets the time zone of a timestamp::

    SELECT timestamp '2012-10-31 01:00 UTC';
    2012-10-31 01:00:00.000 UTC

    SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles';
    2012-10-30 18:00:00.000 America/Los_Angeles

Date and Time Functions
-----------------------

.. function:: current_date -> date

    Returns the current date as of the start of the query.

.. function:: current_time -> time with time zone

    Returns the current time as of the start of the query.

.. function:: current_timestamp -> timestamp with time zone

    Returns the current timestamp as of the start of the query.

.. function:: current_timezone() -> varchar

    Returns the current time zone in the format defined by IANA
    (e.g., ``America/Los_Angeles``) or as fixed offset from UTC (e.g., ``+08:35``)

.. function:: from_iso8601_timestamp(string) -> timestamp with time zone

    Parses the ISO 8601 formatted ``string`` into a ``timestamp with time zone``.

.. function:: from_iso8601_date(string) -> date

    Parses the ISO 8601 formatted ``string`` into a ``date``.

.. function:: from_unixtime(unixtime) -> timestamp

    Returns the UNIX timestamp ``unixtime`` as a timestamp.

.. function:: from_unixtime(unixtime, hours, minutes) -> timestamp with time zone

    Returns the UNIX timestamp ``unixtime`` as a timestamp with time zone
    using ``hours`` and ``minutes`` for the time zone offset.

.. function:: localtime -> time

    Returns the current time as of the start of the query.

.. function:: localtimestamp -> timestamp

    Returns the current timestamp as of the start of the query.

.. function:: now() -> timestamp with time zone

    This is an alias for ``current_timestamp``.

.. function:: to_iso8601(x) -> varchar

    Formats ``x`` as an ISO 8601 string. ``x`` can be date, timestamp, or
    timestamp with time zone.

.. function:: to_unixtime(timestamp) -> double

    Returns ``timestamp`` as a UNIX timestamp.

.. note:: The following SQL-standard functions do not use parenthesis:

    - ``current_date``
    - ``current_time``
    - ``current_timestamp``
    - ``localtime``
    - ``localtimestamp``

Truncation Function
-------------------

The ``date_trunc`` function supports the following units:

=========== ===========================
Unit        Example Truncated Value
=========== ===========================
``second``  ``2001-08-22 03:04:05.000``
``minute``  ``2001-08-22 03:04:00.000``
``hour``    ``2001-08-22 03:00:00.000``
``day``     ``2001-08-22 00:00:00.000``
``week``    ``2001-08-20 00:00:00.000``
``month``   ``2001-08-01 00:00:00.000``
``quarter`` ``2001-07-01 00:00:00.000``
``year``    ``2001-01-01 00:00:00.000``
=========== ===========================

The above examples use the timestamp ``2001-08-22 03:04:05.321`` as the input.

.. function:: date_trunc(unit, x) -> [same as input]

    Returns ``x`` truncated to ``unit``.

Interval Functions
------------------

The functions in this section support the following interval units:

================= ==================
Unit              Description
================= ==================
``millisecond``   Milliseconds
``second``        Seconds
``minute``        Minutes
``hour``          Hours
``day``           Days
``week``          Weeks
``month``         Months
``quarter``       Quarters of a year
``year``          Years
================= ==================

.. function:: date_add(unit, value, timestamp) -> [same as input]

    Adds an interval ``value`` of type ``unit`` to ``timestamp``.
    Subtraction can be performed by using a negative value.

.. function:: date_diff(unit, timestamp1, timestamp2) -> bigint

    Returns ``timestamp2 - timestamp1`` expressed in terms of ``unit``.

MySQL Date Functions
--------------------

The functions in this section use a format string that is compatible with
the MySQL ``date_parse`` and ``str_to_date`` functions. The following table,
based on the MySQL manual, describes the format specifiers:

========= ===========
Specifier Description
========= ===========
``%a``    Abbreviated weekday name (``Sun`` .. ``Sat``)
``%b``    Abbreviated month name (``Jan`` .. ``Dec``)
``%c``    Month, numeric (``0`` .. ``12``)
``%D``    Day of the month with English suffix (``0th``, ``1st``, ``2nd``, ``3rd``, ...)
``%d``    Day of the month, numeric (``00`` .. ``31``)
``%e``    Day of the month, numeric (``0`` .. ``31``)
``%f``    Microseconds (``000000`` .. ``999999``)
``%H``    Hour (``00`` .. ``23``)
``%h``    Hour (``01`` .. ``12``)
``%I``    Hour (``01`` .. ``12``)
``%i``    Minutes, numeric (``00`` .. ``59``)
``%j``    Day of year (``001`` .. ``366``)
``%k``    Hour (``0`` .. ``23``)
``%l``    Hour (``1`` .. ``12``)
``%M``    Month name (``January`` .. ``December``)
``%m``    Month, numeric (``00`` .. ``12``)
``%p``    ``AM`` or ``PM``
``%r``    Time, 12-hour (``hh:mm:ss`` followed by ``AM`` or ``PM``)
``%S``    Seconds (``00`` .. ``59``)
``%s``    Seconds (``00`` .. ``59``)
``%T``    Time, 24-hour (``hh:mm:ss``)
``%U``    Week (``00`` .. ``53``), where Sunday is the first day of the week
``%u``    Week (``00`` .. ``53``), where Monday is the first day of the week
``%V``    Week (``01`` .. ``53``), where Sunday is the first day of the week; used with ``%X``
``%v``    Week (``01`` .. ``53``), where Monday is the first day of the week; used with ``%x``
``%W``    Weekday name (``Sunday`` .. ``Saturday``)
``%w``    Day of the week (``0`` .. ``6``), where Sunday is the first day of the week
``%X``    Year for the week where Sunday is the first day of the week, numeric, four digits; used with ``%V``
``%x``    Year for the week, where Monday is the first day of the week, numeric, four digits; used with ``%v``
``%Y``    Year, numeric, four digits
``%y``    Year, numeric (two digits)
``%%``    A literal ``%`` character
``%x``    ``x``, for any ``x`` not listed above
========= ===========

.. warning:: The following specifiers are not currently supported: ``%D %U %u %V %X``

.. function:: date_format(timestamp, format) -> varchar

    Formats ``timestamp`` as a string using ``format``.

.. function:: date_parse(string, format) -> timestamp

    Parses ``string`` into a timestamp using ``format``.

Java Date Functions
-------------------

The functions in this section use a format string that is compatible with
the Java `SimpleDateFormat`_ pattern format.

.. _SimpleDateFormat: http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html

.. function:: format_datetime(timestamp, format) -> varchar

    Formats ``timestamp`` as a string using ``format``.

.. function:: parse_datetime(string, format) -> timestamp with time zone

    Parses ``string`` into a timestamp with time zone using ``format``.

Extraction Function
-------------------

The ``extract`` function supports the following fields:

=================== ===========
Field               Description
=================== ===========
``YEAR``            :func:`year`
``QUARTER``         :func:`quarter`
``MONTH``           :func:`month`
``WEEK``            :func:`week`
``DAY``             :func:`day`
``DAY_OF_MONTH``    :func:`day`
``DAY_OF_WEEK``     :func:`day_of_week`
``DOW``             :func:`day_of_week`
``DAY_OF_YEAR``     :func:`day_of_year`
``DOY``             :func:`day_of_year`
``YEAR_OF_WEEK``    :func:`year_of_week`
``YOW``             :func:`year_of_week`
``HOUR``            :func:`hour`
``MINUTE``          :func:`minute`
``SECOND``          :func:`second`
``TIMEZONE_HOUR``   :func:`timezone_hour`
``TIMEZONE_MINUTE`` :func:`timezone_minute`
=================== ===========

The types supported by the ``extract`` function vary depending on the
field to be extracted. Most fields support all date and time types.

.. function:: extract(field FROM x) -> bigint

    Returns ``field`` from ``x``.

    .. note:: This SQL-standard function uses special syntax for specifying the arguments.

Convenience Extraction Functions
--------------------------------

.. function:: day(x) -> bigint

    Returns the day of the month from ``x``.

.. function:: day_of_month(x) -> bigint

    This is an alias for :func:`day`.

.. function:: day_of_week(x) -> bigint

    Returns the ISO day of the week from ``x``.
    The value ranges from ``1`` (Monday) to ``7`` (Sunday).

.. function:: day_of_year(x) -> bigint

    Returns the day of the year from ``x``.
    The value ranges from ``1`` to ``366``.

.. function:: dow(x) -> bigint

    This is an alias for :func:`day_of_week`.

.. function:: doy(x) -> bigint

    This is an alias for :func:`day_of_year`.

.. function:: hour(x) -> bigint

    Returns the hour of the day from ``x``.
    The value ranges from ``0`` to ``23``.

.. function:: minute(x) -> bigint

    Returns the minute of the hour from ``x``.

.. function:: month(x) -> bigint

    Returns the month of the year from ``x``.

.. function:: quarter(x) -> bigint

    Returns the quarter of the year from ``x``.
    The value ranges from ``1`` to ``4``.

.. function:: second(x) -> bigint

    Returns the second of the hour from ``x``.

.. function:: timezone_hour(timestamp) -> bigint

    Returns the hour of the time zone offset from ``timestamp``.

.. function:: timezone_minute(timestamp) -> bigint

    Returns the minute of the time zone offset from ``timestamp``.

.. function:: week(x) -> bigint

    Returns the `ISO week`_ of the year from ``x``.
    The value ranges from ``1`` to ``53``.

    .. _ISO week: https://en.wikipedia.org/wiki/ISO_week_date

.. function:: week_of_year(x) -> bigint

    This is an alias for :func:`week`.

.. function:: year(x) -> bigint

    Returns the year from ``x``.

.. function:: year_of_week(x) -> bigint

    Returns the year of the `ISO week`_ from ``x``.

.. function:: yow(x) -> bigint

    This is an alias for :func:`year_of_week`.
