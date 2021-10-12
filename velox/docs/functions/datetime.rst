=====================================
Date and Time Functions
=====================================

.. function:: from_unixtime(unixtime) -> timestamp

    Returns the UNIX timestamp ``unixtime`` as a timestamp.

.. function:: from_unixtime(unixtime, string) -> timestamp with time zone

    Returns the UNIX timestamp ``unixtime`` as a timestamp with time zone
    using ``string`` for the time zone.

... function:: hour(unixtime) -> bigint

    Returns the hour of the day from ``unixtime``. The value ranges from 0 to 23.

.. function:: millisecond(timestamp) -> int64

    Returns the millisecond of the second from ``timestamp``.

.. function:: to_unixtime(timestamp) -> double

    Returns ``timestamp`` as a UNIX timestamp.
