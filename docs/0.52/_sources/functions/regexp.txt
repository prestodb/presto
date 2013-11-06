============================
Regular Expression Functions
============================

All of the regular expression functions use the `Java pattern`_ syntax.

    .. _Java pattern: http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html

.. function:: regexp_extract(string, pattern) -> varchar

    Returns the first substring matched by the regular expression ``pattern``
    in ``string``.

.. function:: regexp_extract(string, pattern, group) -> varchar

    Finds the first occurrence of the regular expression ``pattern`` in
    ``string`` and returns the `capturing group number`_ ``group``.

    .. _capturing group number: http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html#gnumber

.. function:: regexp_like(string, pattern) -> boolean

    Evaluates the regular expression ``pattern`` and determines if it is
    contained within ``string``.

    This function is similar to the ``LIKE`` operator, expect that the
    pattern only needs to be contained within ``string``, rather than
    needing to match all of ``string``. In other words, this performs a
    *contains* operation rather than a *match* operation. You can match
    the entire string by anchoring the pattern using ``^`` and ``$``.

.. function:: regexp_replace(string, pattern) -> varchar

    Removes every instance of the substring matched by the regular expression
    ``pattern`` from ``string``.

.. function:: regexp_replace(string, pattern, replacement) -> varchar

    Replaces every instance of the substring matched by the regular expression
    ``pattern`` in ``string`` with ``replacement``. `Capturing groups`_ can be
    referenced in ``replacement`` using ``$g`` for a numbered group or
    ``${name}`` for a named group. A dollar sign (``$``) may be included in the
    replacement by escaping it with a backslash (``\$``).

    .. _Capturing groups: http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html#cg
