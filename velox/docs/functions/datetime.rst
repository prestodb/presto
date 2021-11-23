=====================================
Date and Time Functions
=====================================

.. function:: from_unixtime(unixtime) -> timestamp

    Returns the UNIX timestamp ``unixtime`` as a timestamp.

.. function:: from_unixtime(unixtime, string) -> timestamp with time zone

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

These functions are supported for TIMESTAMP and DATE values.

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
