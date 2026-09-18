=============
SET TIME ZONE
=============

Synopsis
--------

.. code-block:: none

    SET TIME ZONE LOCAL
    SET TIME ZONE expression

Description
-----------

Sets the time zone for the current session. The time zone affects how timestamp values
are interpreted and displayed in query results.

``SET TIME ZONE LOCAL`` resets the time zone to the session's default time zone.

``SET TIME ZONE expression`` sets the time zone to the value specified by the expression.
The expression can be:

* A string literal representing a time zone name such as ``'America/Los_Angeles'`` or ``'UTC'``. Supported time zone names are listed in the `zone-index.properties <https://github.com/prestodb/presto/blob/master/presto-common/src/main/resources/com/facebook/presto/common/type/zone-index.properties>`_ file.
* A string literal representing a time zone offset such as ``'+01:00'`` or ``'-08:00'``.
* An ``INTERVAL`` literal specifying hours and/or minutes such as ``INTERVAL '10' HOUR`` or ``INTERVAL '-08:00' HOUR TO MINUTE``. The ``HOUR TO MINUTE`` format requires the value to be in ``HH:MM`` format (for example, ``'05:30'`` or ``'-08:00'``).
* A function call that returns a string or interval value.

Time Zone Formats
-----------------

Named Time Zones
^^^^^^^^^^^^^^^^

Time zones can be specified using standard IANA time zone names:

.. code-block:: sql

    SET TIME ZONE 'America/Los_Angeles';
    SET TIME ZONE 'Europe/London';
    SET TIME ZONE 'Asia/Tokyo';
    SET TIME ZONE 'UTC';

Offset Time Zones
^^^^^^^^^^^^^^^^^

Time zones can be specified as UTC offsets in the format ``[+|-]HH:MM``:

.. code-block:: sql

    SET TIME ZONE '+01:00';
    SET TIME ZONE '-08:00';
    SET TIME ZONE '+05:30';

Valid offsets range from ``-14:00`` to ``+14:00``.

Interval Literals
^^^^^^^^^^^^^^^^^

Time zones can be specified using interval literals:

.. code-block:: sql

    -- Hours only
    SET TIME ZONE INTERVAL '10' HOUR;
    SET TIME ZONE INTERVAL '-8' HOUR;

    -- Minutes only
    SET TIME ZONE INTERVAL '30' MINUTE;

    -- Hours and minutes (must use HH:MM format)
    SET TIME ZONE INTERVAL '05:30' HOUR TO MINUTE;
    SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE;

**Note:** Interval offsets must be in whole minutes. Intervals with seconds are not supported. When using ``HOUR TO MINUTE`` format, the value must be specified as ``HH:MM`` (for example, ``'05:30'`` not ``'5:30'``).

Function Calls
^^^^^^^^^^^^^^

Time zones can be set using function calls that return string or interval values:

.. code-block:: sql

    -- String function
    SET TIME ZONE concat('America', '/', 'Los_Angeles');

    -- Interval function (returns INTERVAL DAY TO SECOND)
    -- Note: parse_duration() supports granularity up to nanoseconds, but SET TIME ZONE
    -- only accepts offsets in whole minutes. Values with seconds will be rejected.
    SET TIME ZONE parse_duration('3h');

Examples
--------

Reset to session default:

.. code-block:: sql

    SET TIME ZONE LOCAL;

Set to a named time zone:

.. code-block:: sql

    SET TIME ZONE 'America/New_York';

Set to an offset time zone:

.. code-block:: sql

    SET TIME ZONE '+02:00';

Set using an interval:

.. code-block:: sql

    SET TIME ZONE INTERVAL '-5' HOUR;
    SET TIME ZONE INTERVAL '09:30' HOUR TO MINUTE;

Set using a function:

.. code-block:: sql

    SET TIME ZONE concat('America', '/', 'Chicago');

Verify the current time zone:

.. code-block:: sql

    SELECT current_timezone();

Impact on Queries
-----------------

The time zone setting affects how timestamp values are interpreted and displayed:

.. code-block:: sql

    -- Set time zone to UTC
    SET TIME ZONE 'UTC';
    SELECT TIMESTAMP '2024-01-15 10:00:00';
    -- Returns: 2024-01-15 10:00:00.000 (interpreted in UTC time zone)

    -- Set time zone to Pacific Time
    SET TIME ZONE 'America/Los_Angeles';
    SELECT TIMESTAMP '2024-01-15 10:00:00';
    -- Returns: 2024-01-15 10:00:00.000 (interpreted as Pacific Time)

    -- Time zone affects AT TIME ZONE conversions
    SELECT TIMESTAMP '2024-01-15 10:00:00' AT TIME ZONE 'UTC';
    -- Result depends on current session time zone

Limitations
-----------

* Time zone offsets must be in whole minutes (no seconds component)
* Valid offset range is ``-14:00`` to ``+14:00``
* The time zone setting is session-specific and does not persist across sessions

See Also
--------

:doc:`set-session`, :doc:`show-session`
