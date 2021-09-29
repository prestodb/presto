==============================
String Functions and Operators
==============================

String Operators
----------------

The ``||`` operator performs concatenation.

String Functions
----------------

.. note::

    These functions assume that the input strings contain valid UTF-8 encoded
    Unicode code points.  There are no explicit checks for valid UTF-8 and
    the functions may return incorrect results on invalid UTF-8.
    Invalid UTF-8 data can be corrected with :func:`from_utf8`.

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

.. function:: hamming_distance(string1, string2) -> bigint

    Returns the Hamming distance of ``string1`` and ``string2``,
    i.e. the number of positions at which the corresponding characters are different.
    Note that the two strings must have the same length.

.. function:: length(string) -> bigint

    Returns the length of ``string`` in characters.

.. function:: levenshtein_distance(string1, string2) -> bigint

    Returns the Levenshtein edit distance of ``string1`` and ``string2``,
    i.e. the minimum number of single-character edits (insertions,
    deletions or substitutions) needed to change ``string1`` into ``string2``.

.. function:: lower(string) -> varchar

    Converts ``string`` to lowercase.

.. function:: lpad(string, size, padstring) -> varchar

    Left pads ``string`` to ``size`` characters with ``padstring``.
    If ``size`` is less than the length of ``string``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padstring`` must be non-empty.

.. function:: ltrim(string) -> varchar

    Removes leading whitespace from ``string``.

.. function:: replace(string, search) -> varchar

    Removes all instances of ``search`` from ``string``.

.. function:: replace(string, search, replace) -> varchar

    Replaces all instances of ``search`` with ``replace`` in ``string``.

    If ``search`` is an empty string, inserts ``replace`` in front of every
    character and at the end of the ``string``.

.. function:: reverse(string) -> varchar

    Returns ``string`` with the characters in reverse order.

.. function:: rpad(string, size, padstring) -> varchar

    Right pads ``string`` to ``size`` characters with ``padstring``.
    If ``size`` is less than the length of ``string``, the result is
    truncated to ``size`` characters. ``size`` must not be negative
    and ``padstring`` must be non-empty.

.. function:: rtrim(string) -> varchar

    Removes trailing whitespace from ``string``.

.. function:: split(string, delimiter) -> array(varchar)

    Splits ``string`` on ``delimiter`` and returns an array.

.. function:: split(string, delimiter, limit) -> array(varchar)

    Splits ``string`` on ``delimiter`` and returns an array of size at most
    ``limit``. The last element in the array always contain everything
    left in the ``string``. ``limit`` must be a positive number.

.. function:: split_part(string, delimiter, index) -> varchar

    Splits ``string`` on ``delimiter`` and returns the field ``index``.
    Field indexes start with ``1``. If the index is larger than than
    the number of fields, then null is returned.

.. function:: split_to_map(string, entryDelimiter, keyValueDelimiter) -> map<varchar, varchar>

    Splits ``string`` by ``entryDelimiter`` and ``keyValueDelimiter`` and returns a map.
    ``entryDelimiter`` splits ``string`` into key-value pairs. ``keyValueDelimiter`` splits
    each pair into key and value. Note that ``entryDelimiter`` and ``keyValueDelimiter`` are
    interpreted literally, i.e., as full string matches.

.. function:: split_to_map(string, entryDelimiter, keyValueDelimiter, function(K,V1,V2,R)) -> map<varchar, varchar>

    Splits ``string`` by ``entryDelimiter`` and ``keyValueDelimiter`` and returns a map.
    ``entryDelimiter`` splits ``string`` into key-value pairs. ``keyValueDelimiter`` splits
    each pair into key and value. Note that ``entryDelimiter`` and ``keyValueDelimiter`` are
    interpreted literally, i.e., as full string matches. ``function(K,V1,V2,R)``
    is invoked in cases of duplicate keys to resolve the value that should be in the map.

        SELECT(split_to_map('a:1;b:2;a:3', ';', ':', (k, v1, v2) -> v1)); -- {"a": "1", "b": "2"}
        SELECT(split_to_map('a:1;b:2;a:3', ';', ':', (k, v1, v2) -> CONCAT(v1, v2))); -- {"a": "13", "b": "2"}

.. function:: split_to_multimap(string, entryDelimiter, keyValueDelimiter) -> map(varchar, array(varchar))

    Splits ``string`` by ``entryDelimiter`` and ``keyValueDelimiter`` and returns a map
    containing an array of values for each unique key. ``entryDelimiter`` splits ``string``
    into key-value pairs. ``keyValueDelimiter`` splits each pair into key and value. The
    values for each key will be in the same order as they appeared in ``string``.
    Note that ``entryDelimiter`` and ``keyValueDelimiter`` are interpreted literally,
    i.e., as full string matches.

.. function:: strpos(string, substring) -> bigint

    Returns the starting position of the first instance of ``substring`` in
    ``string``. Positions start with ``1``. If not found, ``0`` is returned.

.. function:: strpos(string, substring, instance) -> bigint

    Returns the position of the N-th ``instance`` of ``substring`` in ``string``.
    ``instance`` must be a positive number.
    Positions start with ``1``. If not found, ``0`` is returned.

.. function:: strrpos(string, substring) -> bigint

    Returns the starting position of the last instance of ``substring`` in ``string``.
    Positions start with ``1``. If not found, ``0`` is returned.

.. function:: strrpos(string, substring, instance) -> bigint

    Returns the position of the N-th ``instance`` of ``substring`` in ``string`` starting from the end of the string.
    ``instance`` must be a positive number.
    Positions start with ``1``. If not found, ``0`` is returned.

.. function:: position(substring IN string) -> bigint

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

    Removes leading and trailing whitespace from ``string``.

.. function:: upper(string) -> varchar

    Converts ``string`` to uppercase.

.. function:: word_stem(word) -> varchar

    Returns the stem of ``word`` in the English language.

.. function:: word_stem(word, lang) -> varchar

    Returns the stem of ``word`` in the ``lang`` language.

Unicode Functions
-----------------

.. function:: normalize(string) -> varchar

    Transforms ``string`` with NFC normalization form.

.. function:: normalize(string, form) -> varchar

    Transforms ``string`` with the specified normalization form.
    ``form`` must be be one of the following keywords:

    ======== ===========
    Form     Description
    ======== ===========
    ``NFD``  Canonical Decomposition
    ``NFC``  Canonical Decomposition, followed by Canonical Composition
    ``NFKD`` Compatibility Decomposition
    ``NFKC`` Compatibility Decomposition, followed by Canonical Composition
    ======== ===========

    .. note::

        This SQL-standard function has special syntax and requires
        specifying ``form`` as a keyword, not as a string.

.. function:: to_utf8(string) -> varbinary

    Encodes ``string`` into a UTF-8 varbinary representation.

.. function:: from_utf8(binary) -> varchar

    Decodes a UTF-8 encoded string from ``binary``. Invalid UTF-8 sequences
    are replaced with the Unicode replacement character ``U+FFFD``.

.. function:: from_utf8(binary, replace) -> varchar

    Decodes a UTF-8 encoded string from ``binary``. Invalid UTF-8 sequences
    are replaced with `replace`. The replacement string `replace` must either
    be a single character or empty (in which case invalid characters are
    removed).

.. function:: key_sampling_percent(varchar) -> double

    Generates a double value between 0.0 and 1.0 based on the hash of the given ``varchar``.
    This function is useful for deterministic sampling of data.

