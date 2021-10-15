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
