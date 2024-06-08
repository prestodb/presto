=====================================
Date and Time Functions and Operators
=====================================

Date and Time Operators
-----------------------

.. list-table::
   :widths: 15 60 25
   :header-rows: 1

   * - Operator
     - Example
     - Result
   * - ``+``
     - ``interval '1' second + interval '1' hour``
     - ``0 01:00:01.000``
   * - ``+``
     - ``timestamp '1970-01-01 00:00:00.000' + interval '1' second``
     - ``1970-01-01 00:00:01.000``
   * - ``-``
     - ``interval '1' hour - interval '1' second``
     - ``0 00:59:59.000``
   * - ``-``
     - ``timestamp '1970-01-01 00:00:00.000' - interval '1' second``
     - ``1969-12-31 23:59:59.000``
   * - ``*``
     - ``interval '1' second * 2``
     - ``0 00:00:02.000``
   * - ``*``
     - ``2 * interval '1' second``
     - ``0 00:00:02.000``
   * - ``*``
     - ``interval '1' second * 0.001``
     - ``0 00:00:00.001``
   * - ``*``
     - ``0.001 * interval '1' second``
     - ``0 00:00:00.001``
   * - ``/``
     - ``interval '15' second / 1.5``
     - ``0 00:00:10.000``

.. function:: plus(x, y) -> [same as x]

    Returns the sum of ``x`` and ``y``. Both ``x`` and ``y`` are intervals day
    to second or one of them can be timestamp. For addition of two intervals day to
    second, returns ``-106751991167 07:12:55.808`` when the addition overflows
    in positive and returns ``106751991167 07:12:55.807`` when the addition
    overflows in negative. When addition of a timestamp with an interval day to
    second, overflowed results are wrapped around.

.. function:: minus(x, y) -> [same as x]

    Returns the result of subtracting ``y`` from ``x``. Both ``x`` and ``y``
    are intervals day to second or ``x`` can be timestamp. For subtraction of
    two intervals day to second, returns ``-106751991167 07:12:55.808`` when
    the subtraction overflows in positive and returns ``106751991167 07:12:55.807``
    when the subtraction overflows in negative. For subtraction of an interval
    day to second from a timestamp, overflowed results are wrapped around.

.. function:: multiply(interval day to second, x) -> interval day to second

    Returns the result of multiplying ``interval day to second`` by ``x``.
    ``x`` can be a bigint or double. Returns ``0`` when ``x`` is NaN. Returns
    ``106751991167 07:12:55.807`` when ``x`` is infinity or when the
    multiplication overflow in positive. Returns ``-106751991167 07:12:55.808``
    when ``x`` is -infinity or when the multiplication overflow in negiative.

.. function:: multiply(x, interval day to second) -> interval day to second

    Returns the result of multiplying ``x`` by ``interval day to second``.
    Same as ``multiply(interval day to second, x)``.

.. function:: divide(interval day to second, x) -> interval day to second

    Returns the result of ``interval day to second`` divided by ``x``. ``x`` is
    a double. Returns ``0`` when ``x`` is NaN or is infinity. Returns
    ``106751991167 07:12:55.807`` when ``x`` is ``0.0`` and
    ``interval day to second`` is not ``0``, or when the division overflows in
    positive. Returns ``-106751991167 07:12:55.808`` when ``x`` is ``-0.0`` and
    ``interval day to second`` is not ``0``, or when the division overflows in
    negiative.

Date and Time Functions
-----------------------

.. function:: current_date() -> date

    Returns the current date.

.. function:: date(x) -> date

    This is an alias for ``CAST(x AS date)``.

.. function:: from_iso8601_date(string) -> date

    Parses the ISO 8601 formatted ``string`` into a ``date``.

    Accepts formats described by the following syntax::

       date = yyyy ['-' MM ['-' dd]]

    Examples of valid input strings:

    * '2012'
    * '2012-4'
    * '2012-04'
    * '2012-4-7'
    * '2012-04-07'
    * '2012-04-07   '

.. function:: from_iso8601_timestamp(string) -> timestamp with time zone

    Parses the ISO 8601 formatted string into a timestamp with time zone.

    Accepts formats described by the following syntax::

        datetime          = time | date-opt-time
        time              = 'T' time-element [offset]
        date-opt-time     = date-element ['T' [time-element] [offset]]
        date-element      = yyyy ['-' MM ['-' dd]]
        time-element      = HH [minute-element] | [fraction]
        minute-element    = ':' mm [second-element] | [fraction]
        second-element    = ':' ss [fraction]
        fraction          = ('.' | ',') digit+
        offset            = 'Z' | (('+' | '-') HH [':' mm [':' ss [('.' | ',') SSS]]])

    Examples of valid input strings:

    * '2012'
    * '2012-4'
    * '2012-04'
    * '2012-4-7'
    * '2012-04-07'
    * '2012-04-07   '
    * '2012-04T01:02'
    * 'T01:02:34'
    * 'T01:02:34,123'
    * '2012-04-07T01:02:34'
    * '2012-04-07T01:02:34.123'
    * '2012-04-07T01:02:34,123'
    * '2012-04-07T01:02:34.123Z'
    * '2012-04-07T01:02:34.123-05:00'

.. function:: from_unixtime(unixtime) -> timestamp

    Returns the UNIX timestamp ``unixtime`` as a timestamp.

.. function:: from_unixtime(unixtime, string) -> timestamp with time zone
    :noindex:

    Returns the UNIX timestamp ``unixtime`` as a timestamp with time zone
    using ``string`` for the time zone.

.. function:: to_iso8601(x) -> varchar

    Formats ``x`` as an ISO 8601 string. Supported types for ``x`` are:
    DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE.

    Example results::

        SELECT to_iso8601(current_date); -- 2024-06-06
        SELECT to_iso8601(now()); -- 2024-06-06T20:25:46.726-07:00
        SELECT to_iso8601(now() + interval '6' month); -- 2024-12-06T20:27:11.992-08:00

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
``week``    ``2001-08-20 00:00:00.000``
``month``   ``2001-08-01 00:00:00.000``
``quarter`` ``2001-07-01 00:00:00.000``
``year``    ``2001-01-01 00:00:00.000``
=========== ===========================

The above examples use the timestamp ``2001-08-22 03:04:05.321`` as the input.

.. function:: date_trunc(unit, x) -> x

    Returns ``x`` truncated to ``unit``. The supported types for ``x`` are TIMESTAMP, DATE, and TIMESTAMP WITH TIME ZONE.

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
``week``        ``Weeks``
``month``       ``Months``
``quarter``     ``Quarters of a year``
``year``        ``Years``
=============== =======================

.. function:: date_add(unit, value, x) -> x

    Adds an interval ``value`` of type ``unit`` to ``x``. The supported types for ``x`` are TIMESTAMP, DATE, and TIMESTAMP WITH TIME ZONE.
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

.. function:: date_format(x, format) -> varchar

    Formats ``x`` as a string using ``format``. ``x`` is a timestamp or a timestamp with time zone.

Java Date Functions
-------------------

The functions in this section leverage a native cpp implementation that follows
a format string compatible with JodaTime’s `DateTimeFormat
<http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html>`_
pattern format. The symbols currently supported are ``y``, ``Y``, ``M`` , ``d``,
``H``, ``m``, ``s``, ``S``, ``z`` and ``Z``.

``z`` represents a timezone name (3-letter format), and ``Z`` a timezone offset
specified using the format ``+00``, ``+00:00`` or ``+0000`` (or ``-``). ``Z``
also accepts ``UTC``,  ``UCT``, ``GMT``, and ``GMT0`` as valid representations
of GMT.

.. function:: format_datetime(timestamp, format) -> varchar

    Formats ``timestamp`` as a string using ``format``.

.. function:: parse_datetime(string, format) -> timestamp with time zone

    Parses string into a timestamp with time zone using ``format``.

Convenience Extraction Functions
--------------------------------

These functions support TIMESTAMP, DATE, and TIMESTAMP WITH TIME ZONE input types.

For these functions, the input timestamp has range limitations on seconds and nanoseconds.
Seconds should be in the range [INT64_MIN/1000 - 1, INT64_MAX/1000], nanoseconds should
be in the range [0, 999999999]. This behavior is different from Presto Java that allows
arbitrary large timestamps.

.. function:: day(x) -> bigint

    Returns the day of the month from ``x``.

    The supported types for ``x`` are DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, INTERVAL DAY TO SECOND.

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

.. function:: last_day_of_month(x) -> date

    Returns the last day of the month.

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

.. function:: timezone_hour(timestamp) -> bigint

    Returns the hour of the time zone offset from ``timestamp``.

.. function:: timezone_minute(timestamp) -> bigint

    Returns the minute of the time zone offset from ``timestamp``.

.. function:: week(x) -> bigint

    Returns the `ISO-Week`_ of the year from x. The value ranges from ``1`` to ``53``.

.. _ISO-Week: https://en.wikipedia.org/wiki/ISO_week_date

.. function:: week_of_year(x) -> bigint

    This is an alias for ``week()``.

.. function:: year(x) -> bigint

    Returns the year from ``x``.

.. function:: year_of_week(x) -> bigint

    Returns the year of the ISO week from ``x``.

.. function:: yow(x) -> bigint

    This is an alias for :func:`year_of_week`.

.. _presto-time-zones:

Time Zones
----------

Velox has full support for time zone rules, which are needed to perform date/time
calculations correctly. Typically, the session time zone is used for temporal
calculations. This is the time zone of the client computer that submits the query, if
available. Otherwise, it is the time zone of the server running the Presto coordinator.

Queries that operate with time zones that follow daylight saving can produce unexpected
results. For example, if we run the following query in the `America/Los Angeles` time
zone: ::

        SELECT date_add('hour', 24, cast('2014-03-08 09:00:00' as timestamp));
        -- 2014-03-09 10:00:00.000

The timestamp appears to only advance 23 hours. This is because on March 9th clocks in
`America/Los Angeles` are turned forward 1 hour, so March 9th only has 23 hours. To
advance the day part of the timestamp, use the `day` unit instead: ::

        SELECT date_add('day', 1, cast('2014-03-08 09:00:00' as timestamp));
        -- 2014-03-09 09:00:00.000

This works because the :func:`date_add` function treats the timestamp as list of fields, adds
the value to the specified field and then rolls any overflow into the next higher field.

Time zones are also necessary for parsing and printing timestamps. Queries that use this
functionality can also produce unexpected results. For example, on the same machine: ::

        SELECT cast('2014-03-09 02:30:00' as timestamp);

The above query causes an error because there was no 2:30 AM on March 9th in
`America/Los_Angeles` due to a daylight saving time transition.

Similarly, the following query has two possible outcomes due to a daylight saving time
transition: ::

        SELECT cast('2014-11-02 01:30:00' as timestamp);
        -- 2014-11-02 08:30:00.000

It can be interpreted as `2014-11-02 01:30:00 PDT`, or `2014-11-02 01:30:00 PST`, which are
`2014-11-02 08:30:00 UTC` or `2014-11-02 09:30:00 UTC` respectively. The former one is
picked to be consistent with Presto.

**Timezone Name Parsing**: When parsing strings that contain timezone names, the
list of supported timezones follow the definition `here
<https://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_.

**Timezone Conversion**: The ``AT TIME ZONE`` operator sets the time zone of a timestamp: ::

        SELECT timestamp '2012-10-31 01:00 UTC';
        -- 2012-10-31 01:00:00.000 UTC

        SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles';
        -- 2012-10-30 18:00:00.000 America/Los_Angeles
