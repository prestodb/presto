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
