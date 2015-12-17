==========
Data Types
==========

Presto currently supports a limited set of data types.
These types can be used with the standard ``CAST`` operator.

BOOLEAN
-------

    This type captures boolean values ``true`` and ``false``.

BIGINT
------

    A 64-bit signed two's complement integer with a minimum value of
    ``-2^63`` and a maximum value of ``2^63 - 1``.

DOUBLE
------

    A double is a 64-bit inexact, variable-precision implementing the
    IEEE Standard 754 for Binary Floating-Point Arithmetic.

VARCHAR
-------

    Variable length character data.

    .. note::

        Character strings with length are not yet supported: ``varchar(n)``

VARBINARY
---------

    Variable length binary data.

    .. note::

        Binary strings with length are not yet supported: ``varbinary(n)``

JSON
----

    Variable length json data.

DATE
----

    Calendar date (year, month, day).

    Example: ``DATE '2001-08-22'``

TIME
----

    Time of day (hour, minute, second, millisecond) without a time zone.
    Values of this type are parsed and rendered in the session time zone.

    Example: ``TIME '01:02:03.456'``

TIME WITH TIME ZONE
-------------------

    Time of day (hour, minute, second, millisecond) with a time zone.
    Values of this type are rendered using the time zone from the value.

    Example: ``TIME '01:02:03.456 America/Los_Angeles'``

TIMESTAMP
---------

    Instant in time that includes the date and time of day without a time zone.
    Values of this type are parsed and rendered in the session time zone.

    Example: ``TIMESTAMP '2001-08-22 03:04:05.321'``

TIMESTAMP WITH TIME ZONE
------------------------

    Instant in time that includes the date and time of day with a time zone.
    Values of this type are rendered using the time zone from the value.

    Example: ``TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles'``

INTERVAL YEAR TO MONTH
----------------------

    Span of years and months.

    Example: ``INTERVAL '3' MONTH``

INTERVAL DAY TO SECOND
----------------------

    Span of days, hours, minutes, seconds and milliseconds.

    Example: ``INTERVAL '2' DAY``

.. _array_type:

ARRAY
-----

    An array of the given component type.

    Example: ``ARRAY[1, 2, 3]``

.. _map_type:

MAP
---

    A map between the given component types.

    Example: ``MAP(ARRAY['foo', 'bar'], ARRAY[1, 2])``

.. _row_type:

ROW
---

    A structure made up of named fields. The fields may be of any SQL type, and are
    accessed with field reference operator ``.``

    Example: ``my_column.my_field``
