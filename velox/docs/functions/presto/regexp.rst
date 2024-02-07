============================
Regular Expression Functions
============================

Regular expression functions use RE2 as the regex engine. RE2 is fast, but
supports only a subset of PCRE syntax and in particular does not support
backtracking and associated features (e.g. back references).
See https://github.com/google/re2/wiki/Syntax for more information.

Compiling regular expressions is CPU intensive. Hence, each function is
limited to 20 different expressions per instance and thread of execution.

.. function:: like(string, pattern) -> boolean
              like(string, pattern, escape) -> boolean

    Evaluates if the ``string`` matches the ``pattern``. Patterns can contain
    regular characters as well as wildcards. Wildcard characters can be escaped
    using the single character specified for the ``escape`` parameter.
    Matching is case sensitive.

    Note: The wildcard '%' represents 0, 1 or multiple characters and the
    wildcard '_' represents exactly one character.

    Note: Each function instance allow for a maximum of 20 regular expressions to
    be compiled per thread of execution. Not all patterns require
    compilation of regular expressions. Patterns 'aaa', 'aaa%', '%aaa', where 'aaa'
    contains only regular characters and '_' wildcards are evaluated without
    using regular expressions. Only those patterns that require the compilation of
    regular expressions are counted towards the limit.

        SELECT like('abc', '%b%'); -- true
        SELECT like('a_c', '%#_%', '#'); -- true

.. function:: regexp_extract(string, pattern) -> varchar

    Returns the first substring matched by the regular expression ``pattern``
    in ``string``::

        SELECT regexp_extract('1a 2b 14m', '\d+'); -- 1

.. function:: regexp_extract(string, pattern, group) -> varchar
    :noindex:

    Finds the first occurrence of the regular expression ``pattern`` in
    ``string`` and returns the capturing group number ``group``::

        SELECT regexp_extract('1a 2b 14m', '(\d+)([a-z]+)', 2); -- 'a'

.. function:: regexp_extract_all(string, pattern) -> array(varchar):

    Returns the substring(s) matched by the regular expression ``pattern``
    in ``string``::

        SELECT regexp_extract_all('1a 2b 14m', '\d+'); -- [1, 2, 14]

.. function:: regexp_extract_all(string, pattern, group) -> array(varchar):
    :noindex:

    Finds all occurrences of the regular expression ``pattern`` in
    ``string`` and returns the capturing group number ``group``::

        SELECT regexp_extract_all('1a 2b 14m', '(\d+)([a-z]+)', 2); -- ['a', 'b', 'm']

.. function:: regexp_like(string, pattern) -> boolean

    Evaluates the regular expression ``pattern`` and determines if it is
    contained within ``string``.

    This function is similar to the ``LIKE`` operator, except that the
    pattern only needs to be contained within ``string``, rather than
    needing to match all of ``string``. In other words, this performs a
    *contains* operation rather than a *match* operation. You can match
    the entire string by anchoring the pattern using ``^`` and ``$``::

        SELECT regexp_like('1a 2b 14m', '\d+b'); -- true

.. function:: regexp_replace(string, pattern) -> varchar

    Removes every instance of the substring matched by the regular expression
    ``pattern`` from ``string``::

        SELECT regexp_replace('1a 2b 14m', '\d+[ab] '); -- '14m'

.. function:: regexp_replace(string, pattern, replacement) -> varchar
    :noindex:

    Replaces every instance of the substring matched by the regular expression
    ``pattern`` in ``string`` with ``replacement``. Capturing groups can be referenced in
    ``replacement`` using ``$g`` for a numbered group or ``${name}`` for a named group. A
    dollar sign (``$``) may be included in the replacement by escaping it with a
    backslash (``\$``)::

        SELECT regexp_replace('1a 2b 14m', '(\d+)([ab]) ', '3c$2 '); -- '3ca 3cb 14m'
