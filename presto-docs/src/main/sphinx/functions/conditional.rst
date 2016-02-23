=======================
Conditional Expressions
=======================

CASE
----

The standard SQL ``CASE`` expression has two forms.
The "simple" form searches each ``value`` expression from left to right
until it finds one that equals ``expression``:

.. code-block:: none

    CASE expression
        WHEN value THEN result
        [ WHEN ... ]
        [ ELSE result ]
    END

The ``result`` for the matching ``value`` is returned.
If no match is found, the ``result`` from the ``ELSE`` clause is
returned if it exists, otherwise null is returned. Example::

    SELECT a,
           CASE a
               WHEN 1 THEN 'one'
               WHEN 2 THEN 'two'
               ELSE 'many'
           END

The "searched" form evaluates each boolean ``condition`` from left
to right until one is true and returns the matching ``result``:

.. code-block:: none

    CASE
        WHEN condition THEN result
        [ WHEN ... ]
        [ ELSE result ]
    END

If no conditions are true, the ``result`` from the ``ELSE`` clause is
returned if it exists, otherwise null is returned. Example::

    SELECT a, b,
           CASE
               WHEN a = 1 THEN 'aaa'
               WHEN b = 2 THEN 'bbb'
               ELSE 'ccc'
           END

IF
--

The ``IF`` function is actually a language construct
that is equivalent to the following ``CASE`` expression:

    .. code-block:: none

        CASE
            WHEN condition THEN true_value
            [ ELSE false_value ]
        END

.. function:: if(condition, true_value)

    Evaluates and returns ``true_value`` if ``condition`` is true,
    otherwise null is returned and ``true_value`` is not evaluated.

.. function:: if(condition, true_value, false_value)

    Evaluates and returns ``true_value`` if ``condition`` is true,
    otherwise evaluates and returns ``false_value``.

COALESCE
--------

.. function:: coalesce(value[, ...])

    Returns the first non-null ``value`` in the argument list.
    Like a ``CASE`` expression, arguments are only evaluated if necessary.

NULLIF
------

.. function:: nullif(value1, value2)

    Returns null if ``value1`` equals ``value2``, otherwise returns ``value1``.

TRY
---

.. function:: try(expression)

    Evaluate an expression and handle certain types of errors by returning
    ``NULL``.

In cases where it is preferable that queries produce ``NULL`` or default values
instead of failing when corrupt or invalid data is encountered, the ``TRY``
function may be useful. To specify default values, the ``TRY`` function can be
used in conjunction with the ``COALESCE`` function.

The following errors are handled by ``TRY``:

* Division by zero
* Invalid cast or function argument
* Numeric value out of range

Examples
~~~~~~~~

Source table with some invalid data:

.. code-block:: sql

    SELECT * FROM shipping;

.. code-block:: none

     origin_state | origin_zip | packages | total_cost
    --------------+------------+----------+------------
     California   |      94131 |       25 |        100
     California   |      P332a |        5 |         72
     California   |      94025 |        0 |        155
     New Jersey   |      08544 |      225 |        490
    (4 rows)

Query failure without ``TRY``:

.. code-block:: sql

    SELECT CAST(origin_zip AS BIGINT) FROM shipping;

.. code-block:: none

    Query failed: Can not cast 'P332a' to BIGINT

``NULL`` values with ``TRY``:

.. code-block:: sql

    SELECT TRY(CAST(origin_zip AS BIGINT)) FROM shipping;

.. code-block:: none

     origin_zip
    ------------
          94131
     NULL
          94025
          08544
    (4 rows)

Query failure without ``TRY``:

.. code-block:: sql

    SELECT total_cost / packages AS per_package FROM shipping;

.. code-block:: none

    Query failed: / by zero

Default values with ``TRY`` and ``COALESCE``:

.. code-block:: sql

    SELECT COALESCE(TRY(total_cost / packages), 0) AS per_package FROM shipping;

.. code-block:: none

     per_package
    -------------
              4
             14
              0
             19
    (4 rows)
