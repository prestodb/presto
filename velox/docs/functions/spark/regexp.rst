============================
Regular Expression Functions
============================

Regular expression functions use RE2 as the regex engine. RE2 is fast, but
supports only a subset of PCRE syntax and in particular does not support
backtracking and associated features (e.g. back references).
See https://github.com/google/re2/wiki/Syntax for more information.

.. spark:function:: regexp_extract(string, pattern) -> varchar

    Returns the first substring matched by the regular expression ``pattern``
    in ``string``. ::

        SELECT regexp_extract('1a 2b 14m', '\d+'); -- 1

.. spark:function:: regexp_extract(string, pattern, group) -> varchar

    Finds the first occurrence of the regular expression ``pattern`` in
    ``string`` and returns the capturing group number ``group``. ::

        SELECT regexp_extract('1a 2b 14m', '(\d+)([a-z]+)', 2); -- 'a'

.. spark:function:: rlike(string, pattern) -> boolean

    Evaluates the regular expression ``pattern`` and determines if it is
    contained within ``string``.

    This function is similar to the ``LIKE`` operator, except that the
    pattern only needs to be contained within ``string``, rather than
    needing to match all of ``string``. In other words, this performs a
    *contains* operation rather than a *match* operation. You can match
    the entire string by anchoring the pattern using ``^`` and ``$``. ::

        SELECT rlike('1a 2b 14m', '\d+b'); -- true
