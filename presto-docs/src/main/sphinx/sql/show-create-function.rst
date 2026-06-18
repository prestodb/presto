====================
SHOW CREATE FUNCTION
====================

Synopsis
--------

.. code-block:: none

    SHOW CREATE FUNCTION function_name [ ( parameter_type[, ...] ) ]

Description
-----------

Show the SQL statement that creates the specified function if the optional list of
parameter types is specified.

If parameter type list is omitted, show one row for each signature with the given ``function_name``.

Examples
--------

Show the SQL statement that can be run to create the ``example.default.array_sum(ARRAY<BIGINT>)`` function::

    SHOW CREATE FUNCTION example.default.array_sum(ARRAY<BIGINT>)

.. code-block:: none

                                               Create Function                                            | Argument Types
    ------------------------------------------------------------------------------------------------------+----------------
     CREATE FUNCTION example.default.array_sum (                                                          | ARRAY(bigint)
        input ARRAY(bigint)                                                                             |
     )                                                                                                    |
     RETURNS bigint                                                                                       |
     COMMENT 'Calculate sum of all array elements. Nulls elements are ignored. Returns 0 on empty array.' |
     LANGUAGE SQL                                                                                         |
     DETERMINISTIC                                                                                        |
     RETURNS NULL ON NULL INPUT                                                                           |
     RETURN "reduce"(input, 0, (s, x) -> (s + COALESCE(x, 0)), (s) -> s)                                  |
    (1 row)

Show all SQL statements that can be run to create the ``example.default.array_sum`` functions::

    SHOW CREATE FUNCTION example.default.array_sum

.. code-block:: none

                                               Create Function                                            | Argument Types
    ------------------------------------------------------------------------------------------------------+----------------
     CREATE FUNCTION example.default.array_sum (                                                         +| ARRAY(bigint)
        input ARRAY(bigint)                                                                            +|
     )                                                                                                   +|
     RETURNS bigint                                                                                      +|
     COMMENT 'Calculate sum of all array elements. Nulls elements are ignored. Returns 0 on empty array.'+|
     LANGUAGE SQL                                                                                        +|
     DETERMINISTIC                                                                                       +|
     RETURNS NULL ON NULL INPUT                                                                          +|
     RETURN "reduce"(input, 0, (s, x) -> (s + COALESCE(x, 0)), (s) -> s)                                  |
     CREATE FUNCTION example.default.array_sum (                                                         +| ARRAY(double)
        input ARRAY(double)                                                                            +|
     )                                                                                                   +|
     RETURNS double                                                                                      +|
     COMMENT 'Calculate sum of all array elements. Nulls elements are ignored. Returns 0 on empty array.'+|
     LANGUAGE SQL                                                                                        +|
     DETERMINISTIC                                                                                       +|
     RETURNS NULL ON NULL INPUT                                                                          +|
     RETURN "reduce"(input, double '0.0', (s, x) -> (s + COALESCE(x, double '0.0')), (s) -> s)            |
    (2 rows)

See Also
--------

:doc:`create-function`
