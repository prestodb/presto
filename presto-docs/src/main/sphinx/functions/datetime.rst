=====================================
Date and Time Functions and Operators
=====================================

.. warning::

    Timestamps are currently represented as UNIX timestamps using the
    ``bigint`` type. This will change in a future release. Timestamps
    should be treated as opaque values and only used with the functions
    and operators in this chapter. Performing integer math on timestamp
    values is guaranteed to break in a future release.

Date and Time Operators
-----------------------

======== =======
Operator Example
======== =======
``+``    ``date '2012-08-08' + interval '1' hour``
``+``    ``time '01:00' + interval '3' hour``
``+``    ``timestamp '2012-08-08 01:00' + interval '23' hour``
``-``    ``date '2012-08-08' - interval '1' hour``
``-``    ``time '01:00' - interval '3' hour``
``-``    ``timestamp '2012-08-08 01:00' - interval '23' hour``
======== =======

Date and Time Functions
-----------------------

.. function:: current_date -> date

    Returns the current date as of the start of the query.

    .. note:: This SQL-standard function does not use parenthesis.

    .. note:: This function is not yet supported.

.. function:: current_time -> time

    Returns the current time as of the start of the query.

    .. note:: This SQL-standard function does not use parenthesis.

    .. note:: This function is not yet supported.

.. function:: current_timestamp -> timestamp

    Returns the current timestamp as of the start of the query.

    .. note:: This SQL-standard function does not use parenthesis.

.. function:: from_unixtime(unixtime) -> timestamp

    Returns the UNIX timestamp ``unixtime`` as a timestamp.

.. function:: now() -> timestamp

    This is an alias for ``current_timestamp``.

.. function:: to_unixtime(timestamp) -> double

    Returns ``timestamp`` as a UNIX timestamp.

Interval Functions
------------------

The functions in this section support the following interval units:

=========== ==================
Unit        Description
=========== ==================
``second``  Seconds
``minute``  Minutes
``hour``    Hours
``day``     Days
``week``    Weeks
``month``   Months
``quarter`` Quarters of a year
``year``    Years
``century`` Centuries
=========== ==================

.. function:: date_add(unit, value, timestamp) -> timestamp

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

.. warning:: The following specifiers are not currently supported: ``%D %U %u %V %X %x``

.. function:: date_format(timestamp, format) -> varchar

    Formats ``timestamp`` as a string using ``format``.

.. function:: date_parse(string, format) -> timestamp

    Parses ``string`` into a timestamp using ``format``.

Java Date Functions
-------------------

The functions in this section use a format string that is compatible with
the Java `SimpleDateFormat`_ pattern format.

.. _SimpleDateFormat: http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html

.. function:: format_datetime(timestamp, format) -> timestamp

    Formats ``timestamp`` as a string using ``format``.

.. function:: parse_datetime(string, format) -> timestamp

    Parses ``string`` into a timestamp using ``format``.

Extraction Function
-------------------

The ``extract`` function supports the following fields:

=================== ===========
Field               Description
=================== ===========
``CENTURY``         :func:`century`
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
``HOUR``            :func:`hour`
``MINUTE``          :func:`minute`
``SECOND``          :func:`second`
``TIMEZONE_HOUR``   Hour component of the time zone offset
``TIMEZONE_MINUTE`` Minute component of the time zone offset
=================== ===========

.. function:: extract(field FROM timestamp) -> bigint

    Returns ``field`` from ``timestamp``.

    .. note:: This SQL-standard function uses special syntax for specifying the arguments.

Convenience Extraction Functions
--------------------------------

.. function:: century(timestamp) -> bigint

    Returns the centry from ``timestamp``.

.. function:: day(timestamp) -> bigint

    Returns the day of the month from ``timestamp``.

.. function:: day_of_month(timestamp) -> bigint

    This is an alias for :func:`day`.

.. function:: day_of_week(timestamp) -> bigint

    Returns the ISO day of the week from ``timestamp``.
    The value ranges from ``1`` (Monday) to ``7`` (Sunday).

.. function:: day_of_year(timestamp) -> bigint

    Returns the day of the year from ``timestamp``.
    The value ranges from ``1`` to ``366``.

.. function:: dow(timestamp) -> bigint

    This is an alias for :func:`day_of_week`.

.. function:: doy(timestamp) -> bigint

    This is an alias for :func:`day_of_year`.

.. function:: hour(timestamp) -> bigint

    Returns the hour of the day from ``timestamp``.
    The value ranges from ``0`` to ``23``.

.. function:: minute(timestamp) -> bigint

    Returns the minute of the hour from ``timestamp``.

.. function:: month(timestamp) -> bigint

    Returns the month of the year from ``timestamp``.

.. function:: quarter(timestamp) -> bigint

    Returns the quarter of the year from ``timestamp``.
    The value ranges from ``1`` to ``4``.

.. function:: second(timestamp) -> bigint

    Returns the second of the hour from ``timestamp``.

.. function:: week(timestamp) -> bigint

    Returns the `ISO week`_ of the year from ``timestamp``.
    The value ranges from ``1`` to ``53``.

    .. _ISO week: https://en.wikipedia.org/wiki/ISO_week_date

.. function:: week_of_year(timestamp) -> bigint

    This is an alias for :func:`week`.

.. function:: year(timestamp) -> bigint

    Returns the year from ``timestamp``.
