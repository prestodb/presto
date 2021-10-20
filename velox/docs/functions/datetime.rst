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
``year``    ``2001-01-01 00:00:00.000``
=========== ===========================

The above examples use the timestamp ``2001-08-22 03:04:05.321`` as the input.

.. function:: date_trunc(unit, timestamp) -> timestamp

    Returns ``timestamp`` truncated to ``unit``.

Convenience Extraction Functions
--------------------------------

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

    Returns the hour of the day from ``timestamp``. The value ranges from 0 to 23.

.. function:: millisecond(timestamp) -> int64

    Returns the millisecond of the second from ``timestamp``.

.. function:: minute(timestamp) -> bigint

    Returns the minute of the hour from ``timestamp``.

.. function:: month(timestamp) -> bigint

    Returns the month of the year from ``timestamp``.

.. function:: second(timestamp) -> bigint

    Returns the second of the minute from ``timestamp``.

.. function:: year(timestamp) -> bigint

    Returns the year from ``timestamp``.
