==============================
String Functions and Operators
==============================

String Operators
----------------

The ``||`` operator performs concatenation.

String Functions
----------------

.. warning::

    Currently, all of the string functions work incorrectly for Unicode (non-ASCII)
    strings. They operate as if strings are a sequence of UTF-8  bytes rather
    than a sequence of Unicode characters. For example, :func:`length` returns
    the number of bytes in the UTF-8 representation of the string rather than
    the number of unicode characters.

.. function:: chr(n) -> varchar

    Returns the Unicode code point ``n`` as a single character string.

.. function:: concat(string1, string2) -> varchar

    Returns the concatenation of ``string1`` and ``string2``.
    This function provides the same functionality as the
    SQL-standard concatenation operator (``||``).

.. function:: length(string) -> bigint

    Returns the length of ``string`` in characters.

.. function:: lower(string) -> varchar

    Converts ``string`` to lowercase.

.. function:: ltrim(string) -> varchar

    Removes leading spaces from ``string``.

.. function:: replace(string, search) -> varchar

    Removes all instances of ``search`` from ``string``.

.. function:: replace(string, search, replace) -> varchar

    Replaces all instances of ``search`` with ``replace`` in ``string``.

.. function:: reverse(string) -> varchar

    Returns ``string`` with the characters in reverse order.

.. function:: rtrim(string) -> varchar

    Removes trailing spaces from ``string``.

.. function:: split_part(string, delimiter, index) -> varchar

    Splits ``string`` on ``delimiter`` and returns the field ``index``.
    Field indexes start with ``1``. If the index is larger than than
    the number of fields, then null is returned.

.. function:: strpos(string, substring) -> bigint

    Returns the starting position of the first instance of ``substring`` in
    ``string``. Positions start with ``1``. If not found, ``0`` is returned.

.. function:: substr(string, start) -> varchar

    Returns the rest of ``string`` from the starting position ``start``.
    Positions start with ``1``. A negative starting position is interpreted
    as being relative to the end of the string.

.. function:: substr(string, start, length) -> varchar

    Returns a substring from ``string`` of length ``length`` from the starting
    position ``start``. Positions start with ``1``. A negative starting
    position is interpreted as being relative to the end of the string.

.. function:: trim(string) -> varchar

    Removes leading and trailing spaces from ``string``.

.. function:: upper(string) -> varchar

    Converts ``string`` to uppercase.
