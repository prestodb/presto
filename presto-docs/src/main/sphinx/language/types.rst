==========
Data Types
==========

Presto has a set of built-in data types, described below.
Additional types can be provided by plugins.

.. note::

    Connectors are not required to support all types.
    See connector documentation for details on supported types.

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Boolean
-------

``BOOLEAN``
^^^^^^^^^^^

    This type captures boolean values ``true`` and ``false``.

Integer
-------

``TINYINT``
^^^^^^^^^^^

    A 8-bit signed two's complement integer with a minimum value of
    ``-2^7`` and a maximum value of ``2^7 - 1``.

``SMALLINT``
^^^^^^^^^^^^

    A 16-bit signed two's complement integer with a minimum value of
    ``-2^15`` and a maximum value of ``2^15 - 1``.

``INTEGER``
^^^^^^^^^^^

    A 32-bit signed two's complement integer with a minimum value of
    ``-2^31`` and a maximum value of ``2^31 - 1``.  The name ``INT`` is
    also available for this type.

``BIGINT``
^^^^^^^^^^

    A 64-bit signed two's complement integer with a minimum value of
    ``-2^63`` and a maximum value of ``2^63 - 1``.

Floating-Point
--------------

``REAL``
^^^^^^^^

    A real is a 32-bit inexact, variable-precision implementing the
    IEEE Standard 754 for Binary Floating-Point Arithmetic.

``DOUBLE``
^^^^^^^^^^

    A double is a 64-bit inexact, variable-precision implementing the
    IEEE Standard 754 for Binary Floating-Point Arithmetic.

Fixed-Precision
---------------

``DECIMAL``
^^^^^^^^^^^

    A fixed precision decimal number. Precision up to 38 digits is supported
    but performance is best up to 18 digits.

    The decimal type takes two literal parameters:

      - **precision** - total number of digits

      - **scale** - number of digits in fractional part. Scale is optional and defaults to 0.

    Example type definitions: ``DECIMAL(10,3)``, ``DECIMAL(20)``

    Example literals: ``DECIMAL '10.3'``, ``DECIMAL '1234567890'``

String
------

``VARCHAR``
^^^^^^^^^^^

    Variable length character data with an optional maximum length.

    Example type definitions: ``varchar``, ``varchar(20)``

``CHAR``
^^^^^^^^

    Fixed length character data. A ``CHAR`` type without length specified has a default length of 1.
    A ``CHAR(x)`` value always has ``x`` characters. For instance, casting ``dog`` to ``CHAR(7)``
    adds 4 implicit trailing spaces. Leading and trailing spaces are included in comparisons of
    ``CHAR`` values. As a result, two character values with different lengths (``CHAR(x)`` and
    ``CHAR(y)`` where ``x != y``) will never be equal.

    Example type definitions: ``char``, ``char(20)``

``VARBINARY``
^^^^^^^^^^^^^

    Variable length binary data.

    .. note::

        Binary strings with length are not yet supported: ``varbinary(n)``

``JSON``
^^^^^^^^

    JSON value type, which can be a JSON object, a JSON array, a JSON number, a JSON string,
    ``true``, ``false`` or ``null``.

Date and Time
-------------

``DATE``
^^^^^^^^

    Calendar date (year, month, day).

    Example: ``DATE '2001-08-22'``

``TIME``
^^^^^^^^

    Time of day (hour, minute, second, millisecond) without a time zone.
    Values of this type are parsed and rendered in the session time zone.

    Example: ``TIME '01:02:03.456'``

``TIME WITH TIME ZONE``
^^^^^^^^^^^^^^^^^^^^^^^

    Time of day (hour, minute, second, millisecond) with a time zone.
    Values of this type are rendered using the time zone from the value.

    Example: ``TIME '01:02:03.456 America/Los_Angeles'``

``TIMESTAMP``
^^^^^^^^^^^^^

    Instant in time that includes the date and time of day without a time zone.
    Values of this type are parsed and rendered in the session time zone.

    Example: ``TIMESTAMP '2001-08-22 03:04:05.321'``

``TIMESTAMP WITH TIME ZONE``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    Instant in time that includes the date and time of day with a time zone.
    Values of this type are rendered using the time zone from the value.

    Example: ``TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles'``

``INTERVAL YEAR TO MONTH``
^^^^^^^^^^^^^^^^^^^^^^^^^^

    Span of years and months.

    Example: ``INTERVAL '3' MONTH``

``INTERVAL DAY TO SECOND``
^^^^^^^^^^^^^^^^^^^^^^^^^^

    Span of days, hours, minutes, seconds and milliseconds.

    Example: ``INTERVAL '2' DAY``

Structural
----------

.. _array_type:

``ARRAY``
^^^^^^^^^

    An array of the given component type.

    Example: ``ARRAY[1, 2, 3]``

.. _map_type:

``MAP``
^^^^^^^

    A map between the given component types.

    Example: ``MAP(ARRAY['foo', 'bar'], ARRAY[1, 2])``

.. _row_type:

``ROW``
^^^^^^^

    A structure made up of named fields. The fields may be of any SQL type, and are
    accessed with field reference operator ``.``

    Example: ``CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))``

Network Address
---------------

.. _ipaddress_type:

``IPADDRESS``
^^^^^^^^^^^^^

    An IP address that can represent either an IPv4 or IPv6 address. Internally,
    the type is a pure IPv6 address. Support for IPv4 is handled using the
    *IPv4-mapped IPv6 address* range (:rfc:`4291#section-2.5.5.2`).
    When creating an ``IPADDRESS``, IPv4 addresses will be mapped into that range.
    When formatting an ``IPADDRESS``, any address within the mapped range will
    be formatted as an IPv4 address. Other addresses will be formatted as IPv6
    using the canonical format defined in :rfc:`5952`.

    Examples: ``IPADDRESS '10.0.0.1'``, ``IPADDRESS '2001:db8::1'``
