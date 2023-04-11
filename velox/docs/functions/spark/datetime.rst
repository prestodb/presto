=====================================
Date and Time Functions
=====================================

Convenience Extraction Functions
--------------------------------

These functions support TIMESTAMP and DATE input types.

.. spark:function:: year(x) -> integer

    Returns the year from ``x``.

.. spark:function:: unix_timestamp() -> integer

    Returns the current UNIX timestamp in seconds.

.. spark:function:: unix_timestamp(string) -> integer

    Returns the UNIX timestamp of time specified by ``string``. Assumes the 
    format ``yyyy-MM-dd HH:mm:ss``. Returns null if ``string`` does not match
    ``format``.

.. spark:function:: unix_timestamp(string, format) -> integer

    Returns the UNIX timestamp of time specified by ``string`` using the
    format described in the ``format`` string. The format follows Spark's
    `Datetime patterns for formatting and parsing
    <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.
    Returns null if ``string`` does not match ``format`` or if ``format``
    is invalid.

.. spark:function:: to_unix_timestamp(string) -> integer

    Alias for ``unix_timestamp(string) -> integer``.

.. spark:function:: to_unix_timestamp(string, format) -> integer

    Alias for ``unix_timestamp(string, format) -> integer``.
