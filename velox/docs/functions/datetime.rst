=====================================
Date and Time Functions
=====================================

.. function:: from_unixtime(unixtime) -> timestamp

    Returns the UNIX timestamp ``unixtime`` as a timestamp.

.. function:: from_unixtime(unixtime, string) -> timestamp with time zone
    :noindex:

    Returns the UNIX timestamp ``unixtime`` as a timestamp with time zone
    using ``string`` for the time zone.

.. function:: to_unixtime(timestamp) -> double

    Returns ``timestamp`` as a UNIX timestamp.

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
``month``   ``2001-08-01 00:00:00.000``
``quarter`` ``2001-07-01 00:00:00.000``
``year``    ``2001-01-01 00:00:00.000``
=========== ===========================

The above examples use the timestamp ``2001-08-22 03:04:05.321`` as the input.

.. function:: date_trunc(unit, x) -> x

    Returns ``x`` truncated to ``unit``. The supported types for ``x`` are TIMESTAMP and DATE.

Interval Functions
------------------

The functions in this section support the following interval units:

=============== =======================
Unit            Description
=============== =======================
``millisecond`` ``Milliseconds``
``second``      ``Seconds``
``minute``      ``Minutes``
``hour``        ``Hours``
``day``         ``Days``
``month``       ``Months``
``quarter``     ``Quarters of a year``
``year``        ``Years``
=============== =======================

.. function:: date_add(unit, value, x) -> x

    Adds an interval ``value`` of type ``unit`` to ``x``. The supported types for ``x`` are TIMESTAMP and DATE.
    Subtraction can be performed by using a negative value.

.. function:: date_diff(unit, x1, x2) -> bigint

    Returns ``x2 - x1`` in terms of ``unit``. The supported types for ``x`` are TIMESTAMP and DATE.

MySQL Date Functions
--------------------

The functions in this section use a format string that is compatible with
the MySQL ``date_parse`` and ``str_to_date`` functions.
The following table, based on the MySQL manual, describes the format specifiers:

========= =============================================================================================================================
Specifier Description
========= =============================================================================================================================
``%a``    Abbreviated weekday name (``Sun`` ... ``Sat``)
``%b``    Abbreviated month name (``Jan`` ... ``Dec``)
``%c``    Month, numeric (``1`` ... ``12``) [4]_
``%D``    Day of the month with English suffix (``0th``, ``1st``, ``2nd``, ``3rd``, ...)
``%d``    Day of the month, numeric (``01`` ... ``31``) [4]_
``%e``    Day of the month, numeric (``1`` ... ``31``) [4]_
``%f``    Fraction of second (6 digits for printing: ``000000`` ... ``999000``; 1 - 9 digits for parsing: ``0`` ... ``999999999``) [1]_
``%H``    Hour (``00`` ... ``23``)
``%h``    Hour (``01`` ... ``12``)
``%I``    Hour (``01`` ... ``12``)
``%i``    Minutes, numeric (``00`` ... ``59``)
``%j``    Day of year (``001`` ... ``366``)
``%k``    Hour (``0`` ... ``23``)
``%l``    Hour (``1`` ... ``12``)
``%M``    Month name (``January`` ... ``December``)
``%m``    Month, numeric (``01`` ... ``12``) [4]_
``%p``    ``AM`` or ``PM``
``%r``    Time, 12-hour (``hh:mm:ss`` followed by ``AM`` or ``PM``)
``%S``    Seconds (``00`` ... ``59``)
``%s``    Seconds (``00`` ... ``59``)
``%T``    Time, 24-hour (``hh:mm:ss``)
``%U``    Week (``00`` ... ``53``), where Sunday is the first day of the week
``%u``    Week (``00`` ... ``53``), where Monday is the first day of the week
``%V``    Week (``01`` ... ``53``), where Sunday is the first day of the week; used with ``%X``
``%v``    Week (``01`` ... ``53``), where Monday is the first day of the week; used with ``%x``
``%W``    Weekday name (``Sunday`` ... ``Saturday``)
``%w``    Day of the week (``0`` ... ``6``), where Sunday is the first day of the week [3]_
``%X``    Year for the week where Sunday is the first day of the week, numeric, four digits; used with ``%V``
``%x``    Year for the week, where Monday is the first day of the week, numeric, four digits; used with ``%v``
``%Y``    Year, numeric, four digits
``%y``    Year, numeric (two digits) [2]_
``%%``    A literal ``%`` character
``%x``    ``x``, for any ``x`` not listed above
========= =============================================================================================================================

.. [1] Timestamp is truncated to milliseconds.

.. [2] When parsing, two-digit year format assumes range ``1970`` ... ``2069``, so “70” will result in year ``1970`` but “69” will produce ``2069``.

.. [3] This specifier is not supported yet. Consider using :func:`day_of_week` (it uses ``1-7`` instead of ``0-6``).

.. [4] This specifier does not support ``0`` as a month or day.

**Warning**: The following specifiers are not currently supported: ``%D``, ``%U``, ``%u``, ``%V``, ``%w``, ``%X``.

.. function:: date_format(timestamp, format) -> varchar

    Formats ``timestamp`` as a string using ``format``.

Java Date Functions
-------------------

The functions in this section leverage a native cpp implementation that follows
a format string compatible with JodaTime’s `DateTimeFormat
<http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html>`_
pattern format.

.. function:: parse_datetime(string, format) -> timestamp with time zone

    Parses string into a timestamp with time zone using ``format``.

Convenience Extraction Functions
--------------------------------

These functions are supported for TIMESTAMP, DATE, and Presto TIMESTAMPWITHTIMEZONE values.

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

    Returns the hour of the day from ``x``. The value ranges from 0 to 23.

.. function:: millisecond(x) -> int64

    Returns the millisecond of the second from ``x``.

.. function:: minute(x) -> bigint

    Returns the minute of the hour from ``x``.

.. function:: month(x) -> bigint

    Returns the month of the year from ``x``.

.. function:: quarter(x) -> bigint

    Returns the quarter of the year from ``x``. The value ranges from ``1`` to ``4``.

.. function:: second(x) -> bigint

    Returns the second of the minute from ``x``.

.. function:: year(x) -> bigint

    Returns the year from ``x``.

.. function:: year_of_week(x) -> bigint

    Returns the year of the ISO week from ``x``.

.. function:: yow(x) -> bigint

    This is an alias for :func:`year_of_week`.
