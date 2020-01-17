====================
Conversion Functions
====================

Presto will implicitly convert numeric and character values to the
correct type if such a conversion is possible. Presto will not convert
between character and numeric types. For example, a query that expects
a varchar will not automatically convert a bigint value to an
equivalent varchar.

When necessary, values can be explicitly cast to a particular type.

Conversion Functions
--------------------

.. function:: cast(value AS type) -> type

    Explicitly cast a value as a type. This can be used to cast a
    varchar to a numeric value type and vice versa.

.. function:: try_cast(value AS type) -> type

    Like :func:`cast`, but returns null if the cast fails.

Data Size
---------

The ``parse_presto_data_size`` function supports the following units:

======= ============= ==============
Unit    Description   Value
======= ============= ==============
``B``   Bytes         1
``kB``  Kilobytes     1024
``MB``  Megabytes     1024\ :sup:`2`
``GB``  Gigabytes     1024\ :sup:`3`
``TB``  Terabytes     1024\ :sup:`4`
``PB``  Petabytes     1024\ :sup:`5`
``EB``  Exabytes      1024\ :sup:`6`
``ZB``  Zettabytes    1024\ :sup:`7`
``YB``  Yottabytes    1024\ :sup:`8`
======= ============= ==============

.. function:: parse_presto_data_size(string) -> decimal(38)

    Parses ``string`` of format ``value unit`` into a number, where
    ``value`` is the fractional number of ``unit`` values::

        SELECT parse_presto_data_size('1B'); -- 1
        SELECT parse_presto_data_size('1kB'); -- 1024
        SELECT parse_presto_data_size('1MB'); -- 1048576
        SELECT parse_presto_data_size('2.3MB'); -- 2411724

Miscellaneous
-------------

.. function:: typeof(expr) -> varchar

    Returns the name of the type of the provided expression::

        SELECT typeof(123); -- integer
        SELECT typeof('cat'); -- varchar(3)
        SELECT typeof(cos(2) + 1.5); -- double
