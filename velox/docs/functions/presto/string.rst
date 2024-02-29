====================================
String Functions
====================================

.. note::

    These functions assume that the input strings contain valid UTF-8 encoded
    Unicode code points. There are no explicit checks for valid UTF-8 and
    the functions may return incorrect results on invalid UTF-8.

    Additionally, the functions operate on Unicode code points and not user
    visible *characters* (or *grapheme clusters*).  Some languages combine
    multiple code points into a single user-perceived *character*, the basic
    unit of a writing system for a language, but the functions will treat each
    code point as a separate unit.

    The :func:`lower` and :func:`upper` functions do not perform
    locale-sensitive, context-sensitive, or one-to-many mappings required for
    some languages. Specifically, this will return incorrect results for
    Lithuanian, Turkish and Azeri.

.. function:: chr(n) -> varchar

    Returns the Unicode code point ``n`` as a single character string.

.. function:: codepoint(string) -> integer

    Returns the Unicode code point of the only character of ``string``.

.. function:: concat(string1, ..., stringN) -> varchar

    Returns the concatenation of ``string1``, ``string2``, ``...``, ``stringN``.
    This function provides the same functionality as the
    SQL-standard concatenation operator (``||``).

.. function:: ends_with(string, substring) -> boolean

    Returns whether ``string`` ends_with with ``substring``.

.. function:: from_utf8(binary) -> varchar

    Decodes a UTF-8 encoded string from ``binary``. Invalid UTF-8 sequences
    are replaced with the Unicode replacement character ``U+FFFD``. Each
    invalid UTF-8 codepoint, including those of multi-byte long, is replaced
    with one replacement character.

.. function:: from_utf8(binary, replace) -> varchar
    :noindex:

    Decodes a UTF-8 encoded string from ``binary``. Invalid UTF-8 sequences are
    replaced with `replace`. Each invalid UTF-8 codepoint, including those of
    multi-byte long, is replaced with one replacement character. The `replace`
    argument can be either Unicode code point (bigint), a single character or
    empty string. When `replace` is an empty string invalid characters are
    removed.

.. function:: length(string) -> bigint

    Returns the length of ``string`` in characters.

.. function:: levenshtein_distance(string_1, string_2) -> bigint

    Returns the Levenshtein edit distance of 2 strings. I.e. the minimum number of single-character edits
    (insertions, deletions or substitutions) needed to convert ``string_1`` to ``string_2``.

.. function:: lower(string) -> varchar

    Converts ``string`` to lowercase.

.. function:: lpad(string, size, padstring) -> varchar

     Left pads ``string`` to ``size`` characters with ``padstring``. If
     ``size`` is less than the length of ``string``, the result is truncated
     to ``size`` characters. ``size`` must not be negative and ``padstring``
     must be non-empty.

.. function:: ltrim(string) -> varchar

    Removes leading whitespace from string. See :func:`trim` for the set of
    recognized whitespace characters.

.. function:: ltrim(string, chars) -> varchar
    :noindex:

    Removes the longest substring containing only characters in ``chars`` from the beginning of the ``string``. ::

        SELECT ltrim('test', 't'); -- est
        SELECT ltrim('tetris', 'te'); -- ris

.. function:: replace(string, search) -> varchar

    Removes all instances of ``search`` from ``string``.

.. function:: replace(string, search, replace) -> varchar
    :noindex:

    Replaces all instances of ``search`` with ``replace`` in ``string``.

    If ``search`` is an empty string, inserts ``replace`` in front of every
    character and at the end of the ``string``.

.. function:: reverse(string) -> varchar
    :noindex:

    Returns input string with characters in reverse order.

.. function:: reverse(varbinary) -> varbinary
    :noindex:

    Returns input binary with bytes in reversed order.

.. function:: rpad(string, size, padstring) -> varchar

     Right pads ``string`` to ``size`` characters with ``padstring``. If
     ``size`` is less than the length of ``string``, the result is truncated
     to ``size`` characters. ``size`` must not be negative and ``padstring``
     must be non-empty.

.. function:: rtrim(string) -> varchar

    Removes trailing whitespace from string. See :func:`trim` for the set of
    recognized whitespace characters.

.. function:: rtrim(string, chars) -> varchar
    :noindex:

    Removes the longest substring containing only characters in ``chars`` from the end of the ``string``. ::

        SELECT rtrim('test', 't'); -- tes
        SELECT rtrim('test...', '.'); -- test

.. function:: split(string, delimiter) -> array(string)

    Splits ``string`` on ``delimiter`` and returns an array.

.. function:: split(string, delimiter, limit) -> array(string)
    :noindex:

    Splits ``string`` on ``delimiter`` and returns an array of size at most ``limit``.

    The last element in the array always contains everything left in the string.
    ``limit`` must be a positive number.

.. function:: split_part(string, delimiter, index) -> string

    Splits ``string`` on ``delimiter`` and returns the part at index ``index``.

    Field indexes start with 1. If the index is larger than the number of fields,
    then null is returned.

.. function:: split_to_map(string, entryDelimiter, keyValueDelimiter) -> map<varchar, varchar>

    Splits ``string`` by ``entryDelimiter`` and ``keyValueDelimiter`` and returns a map.
    ``entryDelimiter`` splits ``string`` into key-value pairs. ``keyValueDelimiter`` splits
    each pair into key and value. Note that ``entryDelimiter`` and ``keyValueDelimiter`` are
    interpreted literally, i.e., as full string matches.

    ``entryDelimiter`` and ``keyValueDelimiter`` must not be empty and must not be the same.
    ``entryDelimiter`` is allowed to be the trailing character.

    Raises an error if there are duplicate keys.

.. function:: starts_with(string, substring) -> boolean

    Returns whether ``string`` starts with ``substring``.

.. function:: strpos(string, substring) -> bigint

    Returns the starting position of the first instance of ``substring`` in
    ``string``. Positions start with ``1``. If not found, ``0`` is returned.

.. function:: strpos(string, substring, instance) -> bigint
    :noindex:

    Returns the position of the N-th ``instance`` of ``substring`` in ``string``.
    ``instance`` must be a positive number.
    Positions start with ``1``. If not found, ``0`` is returned.
    It takes into account overlapping strings when counting occurrences. ::

        SELECT strpos('aaa', 'aa', 2); -- 2

.. function:: strrpos(string, substring) -> bigint

    Returns the starting position of the last instance of ``substring`` in
    ``string``. Positions start with ``1``. If not found, ``0`` is returned.

.. function:: strrpos(string, substring, instance) -> bigint
    :noindex:

    Returns the position of the N-th ``instance`` of ``substring`` in ``string`` starting from the end of the string.
    ``instance`` must be a positive number.
    Positions start with ``1``. If not found, ``0`` is returned.
    It takes into account overlapping strings when counting occurrences. ::

        SELECT strrpos('aaa', 'aa', 2); -- 1

.. function:: substr(string, start) -> varchar

    Returns the rest of ``string`` from the starting position ``start``.
    Positions start with ``1``. A negative starting position is interpreted
    as being relative to the end of the string. Returns empty string if absolute
    value of ``start`` is greater then length of the ``string``.

.. function:: substr(string, start, length) -> varchar
    :noindex:

    Returns a substring from ``string`` of length ``length`` from the starting
    position ``start``. Positions start with ``1``. A negative starting
    position is interpreted as being relative to the end of the string.
    Returns empty string if absolute value of ``'start`` is greater then
    length of the ``string``.

.. function:: trim(string) -> varchar

    Removes starting and ending whitespaces from ``string``.

    Recognized whitespace characters:

    ======  =========================== ======  ===========================
    Code    Description                 Code    Description
    ======  =========================== ======  ===========================
    9       TAB (horizontal tab)        U+1680  Ogham Space Mark
    10      LF (NL line feed, new line) U+2000  En Quad
    11      VT (vertical tab)           U+2001  Em Quad
    12      FF (NP form feed, new page) U+2002  En Space
    13      CR (carriage return)        U+2003  Em Space
    28      FS (file separator)         U+2004  Three-Per-Em Space
    29      GS (group separator)        U+2005  Four-Per-Em Space
    30      RS (record separator)       U+2006  Four-Per-Em Space
    31      US (unit separator)         U+2008  Punctuation Space
    32      Space                       U+2009  Thin Space
    _       _                           U+200a  Hair Space
    _       _                           U+200a  Hair Space
    _       _                           U+2028  Line Separator
    _       _                           U+2029  Paragraph Separator
    _       _                           U+205f  Medium Mathematical Space
    _       _                           U+3000  Ideographic Space
    ======  =========================== ======  ===========================

.. function:: trim(string, chars) -> varchar
    :noindex:

    Removes the longest substring containing only characters in ``chars`` from the beginning and end of the ``string``. ::

        SELECT trim('test', 't'); -- es
        SELECT trim('.t.e.s.t.', '.t'); -- e.s

.. function:: upper(string) -> varchar

    Converts ``string`` to uppercase.

Unicode Functions
-----------------

.. function:: to_utf8(string) -> varbinary

    Encodes ``string`` into a UTF-8 varbinary representation.
