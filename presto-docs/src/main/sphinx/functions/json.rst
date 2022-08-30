============================
JSON Functions and Operators
============================

The SQL standard describes functions and operators to process JSON data. They
allow you to access JSON data according to its structure, generate JSON data,
and store it persistently in SQL tables.

Importantly, the SQL standard imposes that there is no dedicated data type to
represent JSON data in SQL. Instead, JSON data is represented as character or
binary strings. Although Presto supports ``JSON`` type, it is not used or
produced by the following functions.

Presto supports three functions for querying JSON data:
:ref:`json_exists<json_exists>`,
:ref:`json_query<json_query>`, and :ref:`json_value<json_value>`. Each of them
is based on the same mechanism of exploring and processing JSON input using
JSON path.

JSON path language
------------------

The JSON path language is a special language, used exclusively by certain SQL
operators to specify the query to perform on the JSON input. Although JSON path
expressions are embedded in a SQL query, their syntax significantly differs
from SQL. The semantics of predicates, operators, etc. in JSON path expressions
generally follow the semantics of SQL. The JSON path language is case-sensitive
for keywords and identifiers.

.. _json_path_syntax_and_semantics:

JSON path syntax and semantics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A JSON path expression, similar to a SQL expression, is a recursive structure.
Although the name "path" suggests a linear sequence of operations going step by
step deeper into the JSON structure, a JSON path expression is in fact a tree.
It can access the input JSON item multiple times, in multiple ways, and combine
the results. Moreover, the result of a JSON path expression is not a single
item, but an ordered sequence of items. Each of the sub-expressions takes one
or more input sequences, and returns a sequence as the result.

.. note::

    In the lax mode, most path operations first unnest all JSON arrays in the
    input sequence. Any divergence from this rule is mentioned in the following
    listing. Path modes are explained in :ref:`json_path_modes`.

The JSON path language features are divided into: literals, variables,
arithmetic binary expressions, arithmetic unary expressions, and a group of
operators collectively known as accessors.

literals
''''''''

- numeric literals

  They include exact and approximate numbers, and are interpreted as if they
  were SQL values.

.. code-block:: text

    -1, 1.2e3, NaN

- string literals

  They are enclosed in double quotes.

.. code-block:: text

    "Some text"

- boolean literals

.. code-block:: text

    true, false

- null literal

  It has the semantics of the JSON null, not of SQL null. See :ref:`json_comparison_rules`.

.. code-block:: text

    null

variables
'''''''''

- context variable

  It refers to the currently processed input of the JSON
  function.

.. code-block:: text

    $

- named variable

  It refers to a named parameter by its name.

.. code-block:: text

    $param

- current item variable

  It is used inside the filter expression to refer to the currently processed
  item from the input sequence.

.. code-block:: text

    @

- last subscript variable

  It refers to the last index of the innermost enclosing array. Array indexes
  in JSON path expressions are zero-based.

.. code-block:: text

    last

arithmetic binary expressions
'''''''''''''''''''''''''''''

The JSON path language supports five arithmetic binary operators:

.. code-block:: text

    <path1> + <path2>
    <path1> - <path2>
    <path1> * <path2>
    <path1> / <path2>
    <path1> % <path2>

Both operands, ``<path1>`` and ``<path2>``, are evaluated to sequences of
items. For arithmetic binary operators, each input sequence must contain a
single numeric item. The arithmetic operation is performed according to SQL
semantics, and it returns a sequence containing a single element with the
result.

The operators follow the same precedence rules as in SQL arithmetic operations,
and parentheses can be used for grouping.

arithmetic unary expressions
''''''''''''''''''''''''''''

.. code-block:: text

    + <path>
    - <path>

The operand ``<path>`` is evaluated to a sequence of items. Every item must be
a numeric value. The unary plus or minus is applied to every item in the
sequence, following SQL semantics, and the results form the returned sequence.

member accessor
'''''''''''''''

The member accessor returns the value of the member with the specified key for
each JSON object in the input sequence.

.. code-block:: text

    <path>.key
    <path>."key"

The condition when a JSON object does not have such a member is called a
structural error. In the lax mode, it is suppressed, and the faulty object is
excluded from the result.

Let ``<path>`` return a sequence of three JSON objects:

.. code-block:: text

    {"customer" : 100, "region" : "AFRICA"},
    {"region" : "ASIA"},
    {"customer" : 300, "region" : "AFRICA", "comment" : null}

the expression ``<path>.customer`` succeeds in the first and the third object,
but the second object lacks the required member. In strict mode, path
evaluation fails. In lax mode, the second object is silently skipped, and the
resulting sequence is ``100, 300``.

All items in the input sequence must be JSON objects.

.. note::

    Presto does not support JSON objects with duplicate keys.

wildcard member accessor
''''''''''''''''''''''''

Returns values from all key-value pairs for each JSON object in the input
sequence. All the partial results are concatenated into the returned sequence.

.. code-block:: text

    <path>.*

Let ``<path>`` return a sequence of three JSON objects:

.. code-block:: text

    {"customer" : 100, "region" : "AFRICA"},
    {"region" : "ASIA"},
    {"customer" : 300, "region" : "AFRICA", "comment" : null}

The results is:

.. code-block:: text

    100, "AFRICA", "ASIA", 300, "AFRICA", null

All items in the input sequence must be JSON objects.

The order of values returned from a single JSON object is arbitrary. The
sub-sequences from all JSON objects are concatenated in the same order in which
the JSON objects appear in the input sequence.

array accessor
''''''''''''''

Returns the elements at the specified indexes for each JSON array in the input
sequence. Indexes are zero-based.

.. code-block:: text

    <path>[ <subscripts> ]

The ``<subscripts>`` list contains one or more subscripts. Each subscript
specifies a single index or a range (ends inclusive):

.. code-block:: text

    <path>[<path1>, <path2> to <path3>, <path4>,...]

In lax mode, any non-array items resulting from the evaluation of the input
sequence are wrapped into single-element arrays. Note that this is an exception
to the rule of automatic array wrapping.

Each array in the input sequence is processed in the following way:

- The variable ``last`` is set to the last index of the array.
- All subscript indexes are computed in order of declaration. For a
  singleton subscript ``<path1>``, the result must be a singleton numeric item.
  For a range subscript ``<path2> to <path3>``, two numeric items are expected.
- The specified array elements are added in order to the output sequence.

Let ``<path>`` return a sequence of three JSON arrays:

.. code-block:: text

    [0, 1, 2], ["a", "b", "c", "d"], [null, null]

The following expression returns a sequence containing the last element from
every array:

.. code-block:: text

    <path>[last] --> 2, "d", null

The following expression returns the third and fourth element from every array:

.. code-block:: text

    <path>[2 to 3] --> 2, "c", "d"

Note that the first array does not have the fourth element, and the last array
does not have the third or fourth element. Accessing non-existent elements is a
structural error. In strict mode, it causes the path expression to fail. In lax
mode, such errors are suppressed, and only the existing elements are returned.

Another example of a structural error is an improper range specification such
as ``5 to 3``.

Note that the subscripts may overlap, and they do not need to follow the
element order. The order in the returned sequence follows the subscripts:

.. code-block:: text

    <path>[1, 0, 0] --> 1, 0, 0, "b", "a", "a", null, null, null

wildcard array accessor
'''''''''''''''''''''''

Returns all elements of each JSON array in the input sequence.

.. code-block:: text

    <path>[*]

In lax mode, any non-array items resulting from the evaluation of the input
sequence are wrapped into single-element arrays. Note that this is an exception
to the rule of automatic array wrapping.

The output order follows the order of the original JSON arrays. Also, the order
of elements within the arrays is preserved.

Let ``<path>`` return a sequence of three JSON arrays:

.. code-block:: text

    [0, 1, 2], ["a", "b", "c", "d"], [null, null]
    <path>[*] --> 0, 1, 2, "a", "b", "c", "d", null, null

filter
''''''

Retrieves the items from the input sequence which satisfy the predicate.

.. code-block:: text

    <path>?( <predicate> )

JSON path predicates are syntactically similar to boolean expressions in SQL.
However, the semantics are different in many aspects:

- They operate on sequences of items.
- They have their own error handling (they never fail).
- They behave different depending on the lax or strict mode.

The predicate evaluates to ``true``, ``false``, or ``unknown``. Note that some
predicate expressions involve nested JSON path expression. When evaluating the
nested path, the variable ``@`` refers to the currently examined item from the
input sequence.

The following predicate expressions are supported:

- Conjunction

.. code-block:: text

    <predicate1> && <predicate2>

- Disjunction

.. code-block:: text

    <predicate1> || <predicate2>

- Negation

.. code-block:: text

    ! <predicate>

- ``exists`` predicate

.. code-block:: text

    exists( <path> )

Returns ``true`` if the nested path evaluates to a non-empty sequence, and
``false`` when the nested path evaluates to an empty sequence. If the path
evaluation throws an error, returns ``unknown``.

- ``starts with`` predicate

.. code-block:: text

    <path> starts with "Some text"
    <path> starts with $variable

The nested ``<path>`` must evaluate to a sequence of textual items, and the
other operand must evaluate to a single textual item. If evaluating of either
operand throws an error, the result is ``unknown``. All items from the sequence
are checked for starting with the right operand. The result is ``true`` if a
match is found, otherwise ``false``. However, if any of the comparisons throws
an error, the result in the strict mode is ``unknown``. The result in the lax
mode depends on whether the match or the error was found first.

- ``is unknown`` predicate

.. code-block:: text

    ( <predicate> ) is unknown

Returns ``true`` if the nested predicate evaluates to ``unknown``, and
``false`` otherwise.

- Comparisons

.. code-block:: text

    <path1> == <path2>
    <path1> <> <path2>
    <path1> != <path2>
    <path1> < <path2>
    <path1> > <path2>
    <path1> <= <path2>
    <path1> >= <path2>

Both operands of a comparison evaluate to sequences of items. If either
evaluation throws an error, the result is ``unknown``. Items from the left and
right sequence are then compared pairwise. Similarly to the ``starts with``
predicate, the result is ``true`` if any of the comparisons returns ``true``,
otherwise ``false``. However, if any of the comparisons throws an error, for
example because the compared types are not compatible, the result in the strict
mode is ``unknown``. The result in the lax mode depends on whether the ``true``
comparison or the error was found first.

.. _json_comparison_rules:

Comparison rules
****************

Null values in the context of comparison behave different than SQL null:

- null == null --> ``true``
- null != null, null < null, ... --> ``false``
- null compared to a scalar value --> ``false``
- null compared to a JSON array or a JSON object --> ``false``

When comparing two scalar values, ``true`` or ``false`` is returned if the
comparison is successfully performed. The semantics of the comparison is the
same as in SQL. In case of an error, e.g. comparing text and number,
``unknown`` is returned.

Comparing a scalar value with a JSON array or a JSON object, and comparing JSON
arrays/objects is an error, so ``unknown`` is returned.

Examples of filter
******************

Let ``<path>`` return a sequence of three JSON objects:

.. code-block:: text

    {"customer" : 100, "region" : "AFRICA"},
    {"region" : "ASIA"},
    {"customer" : 300, "region" : "AFRICA", "comment" : null}

.. code-block:: text

    <path>?(@.region != "ASIA") --> {"customer" : 100, "region" : "AFRICA"},
                                    {"customer" : 300, "region" : "AFRICA", "comment" : null}
    <path>?(!exists(@.customer)) --> {"region" : "ASIA"}

The following accessors are collectively referred to as **item methods**.

double()
''''''''

Converts numeric or text values into double values.

.. code-block:: text

    <path>.double()

Let ``<path>`` return a sequence ``-1, 23e4, "5.6"``:

.. code-block:: text

    <path>.double() --> -1e0, 23e4, 5.6e0

ceiling(), floor(), and abs()
'''''''''''''''''''''''''''''

Gets the ceiling, the floor or the absolute value for every numeric item in the
sequence. The semantics of the operations is the same as in SQL.

Let ``<path>`` return a sequence ``-1.5, -1, 1.3``:

.. code-block:: text

    <path>.ceiling() --> -1.0, -1, 2.0
    <path>.floor() --> -2.0, -1, 1.0
    <path>.abs() --> 1.5, 1, 1.3

keyvalue()
''''''''''

Returns a collection of JSON objects including one object per every member of
the original object for every JSON object in the sequence.

.. code-block:: text

    <path>.keyvalue()

The returned objects have three members:

- "name", which is the original key,
- "value", which is the original bound value,
- "id", which is the unique number, specific to an input object.

Let ``<path>`` be a sequence of three JSON objects:

.. code-block:: text

    {"customer" : 100, "region" : "AFRICA"},
    {"region" : "ASIA"},
    {"customer" : 300, "region" : "AFRICA", "comment" : null}

.. code-block:: text

    <path>.keyvalue() --> {"name" : "customer", "value" : 100, "id" : 0},
                          {"name" : "region", "value" : "AFRICA", "id" : 0},
                          {"name" : "region", "value" : "ASIA", "id" : 1},
                          {"name" : "customer", "value" : 300, "id" : 2},
                          {"name" : "region", "value" : "AFRICA", "id" : 2},
                          {"name" : "comment", "value" : null, "id" : 2}

It is required that all items in the input sequence are JSON objects.

The order of the returned values follows the order of the original JSON
objects. However, within objects, the order of returned entries is arbitrary.

type()
''''''

Returns a textual value containing the type name for every item in the
sequence.

.. code-block:: text

    <path>.type()

This method does not perform array unwrapping in the lax mode.

The returned values are:

- ``"null"`` for JSON null,
- ``"number"`` for a numeric item,
- ``"string"`` for a textual item,
- ``"boolean"`` for a boolean item,
- ``"date"`` for an item of type date,
- ``"time without time zone"`` for an item of type time,
- ``"time with time zone"`` for an item of type time with time zone,
- ``"timestamp without time zone"`` for an item of type timestamp,
- ``"timestamp with time zone"`` for an item of type timestamp with time zone,
- ``"array"`` for JSON array,
- ``"object"`` for JSON object,

size()
''''''

Returns a numeric value containing the size for every JSON array in the
sequence.

.. code-block:: text

    <path>.size()

This method does not perform array unwrapping in the lax mode. Instead, all
non-array items are wrapped in singleton JSON arrays, so their size is ``1``.

It is required that all items in the input sequence are JSON arrays.

Let ``<path>`` return a sequence of three JSON arrays:

.. code-block:: text

    [0, 1, 2], ["a", "b", "c", "d"], [null, null]
    <path>.size() --> 3, 4, 2

Limitations
^^^^^^^^^^^

The SQL standard describes the ``datetime()`` JSON path item method and the
``like_regex()`` JSON path predicate. Presto does not support them.

.. _json_path_modes:

JSON path modes
^^^^^^^^^^^^^^^

The JSON path expression can be evaluated in two modes: strict and lax. In the
strict mode, it is required that the input JSON data strictly fits the schema
required by the path expression. In the lax mode, the input JSON data can
diverge from the expected schema.

The following table shows the differences between the two modes.

.. list-table::
   :widths: 40 20 40
   :header-rows: 1

   * - Condition
     - strict mode
     - lax mode
   * - Performing an operation which requires a non-array on an array, e.g.:

       ``$.key`` requires a JSON object

       ``$.floor()`` requires a numeric value
     - ERROR
     - The array is automatically unnested, and the operation is performed on
       each array element.
   * - Performing an operation which requires an array on an non-array, e.g.:

       ``$[0]``, ``$[*]``, ``$.size()``
     - ERROR
     - The non-array item is automatically wrapped in a singleton array, and
       the operation is performed on the array.
   * - A structural error: accessing a non-existent element of an array or a
       non-existent member of a JSON object, e.g.:

       ``$[-1]`` (array index out of bounds)

       ``$.key``, where the input JSON object does not have a member ``key``
     - ERROR
     - The error is suppressed, and the operation results in an empty sequence.

Examples of the lax mode behavior
'''''''''''''''''''''''''''''''''

Let ``<path>`` return a sequence of three items, a JSON array, a JSON object,
and a scalar numeric value:

.. code-block:: text

    [1, "a", null], {"key1" : 1.0, "key2" : true}, -2e3

The following example shows the wildcard array accessor in the lax mode. The
JSON array returns all its elements, while the JSON object and the number are
wrapped in singleton arrays and then unnested, so effectively they appear
unchanged in the output sequence:

.. code-block:: text

    <path>[*] --> 1, "a", null, {"key1" : 1.0, "key2" : true}, -2e3

When calling the ``size()`` method, the JSON object and the number are also
wrapped in singleton arrays:

.. code-block:: text

    <path>.size() --> 3, 1, 1

In some cases, the lax mode cannot prevent failure. In the following example,
even though the JSON array is unwrapped prior to calling the ``floor()``
method, the item ``"a"`` causes type mismatch.

.. code-block:: text

    <path>.floor() --> ERROR

.. _json_exists:

json_exists
-----------

The ``json_exists`` function determines whether a JSON value satisfies a JSON
path specification.

.. code-block:: text

    JSON_EXISTS(
        json_input [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ],
        json_path
        [ PASSING json_argument [, ...] ]
        [ { TRUE | FALSE | UNKNOWN | ERROR } ON ERROR ]
        )

The ``json_path`` is evaluated using the ``json_input`` as the context variable
(``$``), and the passed arguments as the named variables (``$variable_name``).
The returned value is ``true`` if the path returns a non-empty sequence, and
``false`` if the path returns an empty sequence. If an error occurs, the
returned value depends on the ``ON ERROR`` clause. The default value returned
``ON ERROR`` is ``FALSE``. The ``ON ERROR`` clause is applied for the following
kinds of errors:

- Input conversion errors, such as malformed JSON
- JSON path evaluation errors, e.g. division by zero

``json_input`` is a character string or a binary string. It should contain
a single JSON item. For a binary string, you can specify encoding.

``json_path`` is a string literal, containing the path mode specification, and
the path expression, following the syntax rules described in
:ref:`json_path_syntax_and_semantics`.

.. code-block:: text

    'strict ($.price + $.tax)?(@ > 99.9)'
    'lax $[0 to 1].floor()?(@ > 10)'

In the ``PASSING`` clause you can pass arbitrary expressions to be used by the
path expression.

.. code-block:: text

    PASSING orders.totalprice AS O_PRICE,
            orders.tax % 10 AS O_TAX

The passed parameters can be referenced in the path expression by named
variables, prefixed with ``$``.

.. code-block:: text

    'lax $?(@.price > $O_PRICE || @.tax > $O_TAX)'

Additionally to SQL values, you can pass JSON values, specifying the format and
optional encoding:

.. code-block:: text

    PASSING orders.json_desc FORMAT JSON AS o_desc,
            orders.binary_record FORMAT JSON ENCODING UTF16 AS o_rec

Note that the JSON path language is case-sensitive, while the unquoted SQL
identifiers are upper-cased. Therefore, it is recommended to use quoted
identifiers in the ``PASSING`` clause:

.. code-block:: text

    'lax $.$KeyName' PASSING nation.name AS KeyName --> ERROR; no passed value found
    'lax $.$KeyName' PASSING nation.name AS "KeyName" --> correct

Examples
^^^^^^^^

Let ``customers`` be a table containing two columns: ``id:bigint``,
``description:varchar``.

========== ======================================================
id         description
========== ======================================================
101        '{"comment" : "nice", "children" : [10, 13, 16]}'
102        '{"comment" : "problematic", "children" : [8, 11]}'
103        '{"comment" : "knows best", "children" : [2]}'
========== ======================================================

The following query checks which customers have children above the age of 10:

.. code-block:: text

    SELECT
          id,
          json_exists(
                      description,
                      'lax $.children[*]?(@ > 10)'
                     ) AS children_above_ten
    FROM customers

========== ====================
id         children_above_ten
========== ====================
101        true
102        true
103        false
========== ====================

In the following query, the path mode is strict. We check the third child for
each customer. This should cause a structural error for the customers who do
not have three or more children. This error is handled according to the ``ON
ERROR`` clause.

.. code-block:: text

    SELECT
          id,
          json_exists(
                      description,
                      'strict $.children[2]?(@ > 10)'
                      UNKNOWN ON ERROR
                     ) AS child_3_above_ten
    FROM customers

========== ==================
id         child_3_above_ten
========== ==================
101        true
102        NULL
103        NULL
========== ==================

.. _json_query:

json_query
----------

The ``json_query`` function extracts a JSON value from a JSON value.

.. code-block:: text

    JSON_QUERY(
        json_input [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ],
        json_path
        [ PASSING json_argument [, ...] ]
        [ RETURNING type [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ] ]
        [ WITHOUT [ ARRAY ] WRAPPER |
          WITH [ { CONDITIONAL | UNCONDITIONAL } ] [ ARRAY ] WRAPPER ]
        [ { KEEP | OMIT } QUOTES [ ON SCALAR STRING ] ]
        [ { ERROR | NULL | EMPTY ARRAY | EMPTY OBJECT } ON EMPTY ]
        [ { ERROR | NULL | EMPTY ARRAY | EMPTY OBJECT } ON ERROR ]
        )

The ``json_path`` is evaluated using the ``json_input`` as the context variable
(``$``), and the passed arguments as the named variables (``$variable_name``).

The returned value is a JSON item returned by the path. By default, it is
represented as a character string (``varchar``). In the ``RETURNING`` clause,
you can specify other character string type or ``varbinary``. With
``varbinary``, you can also specify the desired encoding.

``json_input`` is a character string or a binary string. It should contain
a single JSON item. For a binary string, you can specify encoding.

``json_path`` is a string literal, containing the path mode specification, and
the path expression, following the syntax rules described in
:ref:`json_path_syntax_and_semantics`.

.. code-block:: text

    'strict $.keyvalue()?(@.name == $cust_id)'
    'lax $[5 to last]'

In the ``PASSING`` clause you can pass arbitrary expressions to be used by the
path expression.

.. code-block:: text

    PASSING orders.custkey AS CUST_ID

The passed parameters can be referenced in the path expression by named
variables, prefixed with ``$``.

.. code-block:: text

    'strict $.keyvalue()?(@.value == $CUST_ID)'

Additionally to SQL values, you can pass JSON values, specifying the format and
optional encoding:

.. code-block:: text

    PASSING orders.json_desc FORMAT JSON AS o_desc,
            orders.binary_record FORMAT JSON ENCODING UTF16 AS o_rec

Note that the JSON path language is case-sensitive, while the unquoted SQL
identifiers are upper-cased. Therefore, it is recommended to use quoted
identifiers in the ``PASSING`` clause:

.. code-block:: text

    'lax $.$KeyName' PASSING nation.name AS KeyName --> ERROR; no passed value found
    'lax $.$KeyName' PASSING nation.name AS "KeyName" --> correct

The ``ARRAY WRAPPER`` clause lets you modify the output by wrapping the results
in a JSON array. ``WITHOUT ARRAY WRAPPER`` is the default option. ``WITH
CONDITIONAL ARRAY WRAPPER`` wraps every result which is not a singleton JSON
array or JSON object. ``WITH UNCONDITIONAL ARRAY WRAPPER`` wraps every result.

The ``QUOTES`` clause lets you modify the result for a scalar string by
removing the double quotes being part of the JSON string representation.

Examples
^^^^^^^^

Let ``customers`` be a table containing two columns: ``id:bigint``,
``description:varchar``.

========== ======================================================
id         description
========== ======================================================
101        '{"comment" : "nice", "children" : [10, 13, 16]}'
102        '{"comment" : "problematic", "children" : [8, 11]}'
103        '{"comment" : "knows best", "children" : [2]}'
========== ======================================================

The following query gets the ``children`` array for each customer:

.. code-block:: text

    SELECT
          id,
          json_query(
                     description,
                     'lax $.children'
                    ) AS children
    FROM customers

========== ================
id         children
========== ================
101        '[10,13,16]'
102        '[8,11]'
103        '[2]'
========== ================

The following query gets the collection of children for each customer.
Note that the ``json_query`` function can only output a single JSON item. If
you don't use array wrapper, you get an error for every customer with multiple
children. The error is handled according to the ``ON ERROR`` clause.

.. code-block:: text

    SELECT
          id,
          json_query(
                     description,
                     'lax $.children[*]'
                     WITHOUT ARRAY WRAPPER
                     NULL ON ERROR
                    ) AS children
    FROM customers

========== ================
id         children
========== ================
101        NULL
102        NULL
103        '2'
========== ================

The following query gets the last child for each customer, wrapped in a JSON
array:

.. code-block:: text

    SELECT
          id,
          json_query(
                     description,
                     'lax $.children[last]'
                     WITH ARRAY WRAPPER
                    ) AS last_child
    FROM customers

========== ================
id         last_child
========== ================
101        '[16]'
102        '[11]'
103        '[2]'
========== ================

The following query gets all children above the age of 12 for each customer,
wrapped in a JSON array. The second and the third customer don't have children
of this age. Such case is handled according to the ``ON EMPTY`` clause. The
default value returned ``ON EMPTY`` is ``NULL``. In the following example,
``EMPTY ARRAY ON EMPTY`` is specified.

.. code-block:: text

    SELECT
          id,
          json_query(
                     description,
                     'strict $.children[*]?(@ > 12)'
                     WITH ARRAY WRAPPER
                     EMPTY ARRAY ON EMPTY
                    ) AS children
    FROM customers

========== ================
id         children
========== ================
101        '[13,16]'
102        '[]'
103        '[]'
========== ================

The following query shows the result of the ``QUOTES`` clause. Note that ``KEEP
QUOTES`` is the default.

.. code-block:: text

    SELECT
          id,
          json_query(description, 'strict $.comment' KEEP QUOTES) AS quoted_comment,
          json_query(description, 'strict $.comment' OMIT QUOTES) AS unquoted_comment
    FROM customers

========== ================ ================
id         quoted_comment   unquoted_comment
========== ================ ================
101        '"nice"'         'nice'
102        '"problematic"'  'problematic'
103        '"knows best"'   'knows best'
========== ================ ================

If an error occurs, the returned value depends on the ``ON ERROR`` clause. The
default value returned ``ON ERROR`` is ``NULL``. One example of error is
multiple items returned by the path. Other errors caught and handled according
to the ``ON ERROR`` clause are:

- Input conversion errors, such as malformed JSON
- JSON path evaluation errors, e.g. division by zero
- Output conversion errors

.. _json_value:

json_value
----------

The ``json_value`` function extracts an SQL scalar from a JSON value.

.. code-block:: text

    JSON_VALUE(
        json_input [ FORMAT JSON [ ENCODING { UTF8 | UTF16 | UTF32 } ] ],
        json_path
        [ PASSING json_argument [, ...] ]
        [ RETURNING type ]
        [ { ERROR | NULL | DEFAULT expression } ON EMPTY ]
        [ { ERROR | NULL | DEFAULT expression } ON ERROR ]
        )

The ``json_path`` is evaluated using the ``json_input`` as the context variable
(``$``), and the passed arguments as the named variables (``$variable_name``).

The returned value is the SQL scalar returned by the path. By default, it is
converted to string (``varchar``). In the ``RETURNING`` clause, you can specify
other desired type: a character string type, numeric, boolean or datetime type.

``json_input`` is a character string or a binary string. It should contain
a single JSON item. For a binary string, you can specify encoding.

``json_path`` is a string literal, containing the path mode specification, and
the path expression, following the syntax rules described in
:ref:`json_path_syntax_and_semantics`.

.. code-block:: text

    'strict $.price + $tax'
    'lax $[last].abs().floor()'

In the ``PASSING`` clause you can pass arbitrary expressions to be used by the
path expression.

.. code-block:: text

    PASSING orders.tax AS O_TAX

The passed parameters can be referenced in the path expression by named
variables, prefixed with ``$``.

.. code-block:: text

    'strict $[last].price + $O_TAX'

Additionally to SQL values, you can pass JSON values, specifying the format and
optional encoding:

.. code-block:: text

    PASSING orders.json_desc FORMAT JSON AS o_desc,
            orders.binary_record FORMAT JSON ENCODING UTF16 AS o_rec

Note that the JSON path language is case-sensitive, while the unquoted SQL
identifiers are upper-cased. Therefore, it is recommended to use quoted
identifiers in the ``PASSING`` clause:

.. code-block:: text

    'lax $.$KeyName' PASSING nation.name AS KeyName --> ERROR; no passed value found
    'lax $.$KeyName' PASSING nation.name AS "KeyName" --> correct

If the path returns an empty sequence, the ``ON EMPTY`` clause is applied. The
default value returned ``ON EMPTY`` is ``NULL``. You can also specify the
default value:

.. code-block:: text

    DEFAULT -1 ON EMPTY

If an error occurs, the returned value depends on the ``ON ERROR`` clause. The
default value returned ``ON ERROR`` is ``NULL``. One example of error is
multiple items returned by the path. Other errors caught and handled according
to the ``ON ERROR`` clause are:

- Input conversion errors, such as malformed JSON
- JSON path evaluation errors, e.g. division by zero
- Returned scalar not convertible to the desired type

Examples
^^^^^^^^

Let ``customers`` be a table containing two columns: ``id:bigint``,
``description:varchar``.

========== ======================================================
id         description
========== ======================================================
101        '{"comment" : "nice", "children" : [10, 13, 16]}'
102        '{"comment" : "problematic", "children" : [8, 11]}'
103        '{"comment" : "knows best", "children" : [2]}'
========== ======================================================

The following query gets the ``comment`` for each customer as ``char(12)``:

.. code-block:: text

    SELECT id, json_value(
                          description,
                          'lax $.comment'
                          RETURNING char(12)
                         ) AS comment
    FROM customers

========== ================
id         comment
========== ================
101        'nice        '
102        'problematic '
103        'knows best  '
========== ================

The following query gets the first child's age for each customer as
``tinyint``:

.. code-block:: text

    SELECT id, json_value(
                          description,
                          'lax $.children[0]'
                          RETURNING tinyint
                         ) AS child
    FROM customers

========== ================
id         child
========== ================
101        10
102        8
103        2
========== ================

The following query gets the third child's age for each customer. In the strict
mode, this should cause a structural error for the customers who do not have
the third child. This error is handled according to the ``ON ERROR`` clause.

.. code-block:: text

    SELECT id, json_value(
                          description,
                          'strict $.children[2]'
                          DEFAULT 'err' ON ERROR
                         ) AS child
    FROM customers

========== ================
id         child
========== ================
101        '16'
102        'err'
103        'err'
========== ================

After changing the mode to lax, the structural error is suppressed, and the
customers without a third child produce empty sequence. This case is handled
according to the ``ON EMPTY`` clause.

.. code-block:: text

    SELECT id, json_value(
                          description,
                          'lax $.children[2]'
                          DEFAULT 'missing' ON EMPTY
                         ) AS child
    FROM customers

========== ================
id         child
========== ================
101        '16'
102        'missing'
103        'missing'
========== ================

.. warning::

    The following functions and operators are not compliant with the SQL
    standard, and should be considered deprecated. According to the SQL
    standard, there shall be no ``JSON`` data type. Instead, JSON values
    should be represented as string values. The remaining functionality of the
    following functions is covered by the functions described previously.

Cast to JSON
------------

    Casting from ``BOOLEAN``, ``TINYINT``, ``SMALLINT``, ``INTEGER``,
    ``BIGINT``, ``REAL``, ``DOUBLE`` or ``VARCHAR`` is supported.
    Casting from ``ARRAY``, ``MAP`` or ``ROW`` is supported when the element type of
    the array is one of the supported types, or when the key type of the map
    is ``VARCHAR`` and value type of the map is one of the supported types,
    or when every field type of the row is one of the supported types.
    Behaviors of the casts are shown with the examples below::

        SELECT CAST(NULL AS JSON); -- NULL
        SELECT CAST(1 AS JSON); -- JSON '1'
        SELECT CAST(9223372036854775807 AS JSON); -- JSON '9223372036854775807'
        SELECT CAST('abc' AS JSON); -- JSON '"abc"'
        SELECT CAST(true AS JSON); -- JSON 'true'
        SELECT CAST(1.234 AS JSON); -- JSON '1.234'
        SELECT CAST(ARRAY[1, 23, 456] AS JSON); -- JSON '[1,23,456]'
        SELECT CAST(ARRAY[1, NULL, 456] AS JSON); -- JSON '[1,null,456]'
        SELECT CAST(ARRAY[ARRAY[1, 23], ARRAY[456]] AS JSON); -- JSON '[[1,23],[456]]'
        SELECT CAST(MAP_FROM_ENTRIES(ARRAY[('k1', 1), ('k2', 23), ('k3', 456)]) AS JSON); -- JSON '{"k1":1,"k2":23,"k3":456}'
        SELECT CAST(CAST(ROW(123, 'abc', true) AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)) AS JSON); -- JSON '[123,"abc",true]'

.. note::

    Casting from NULL to ``JSON`` is not straightforward. Casting
    from a standalone ``NULL`` will produce a SQL ``NULL`` instead of
    ``JSON 'null'``. However, when casting from arrays or map containing
    ``NULL``\s, the produced ``JSON`` will have ``null``\s in it.

.. note::

    When casting from ``ROW`` to ``JSON``, the result is a JSON array rather
    than a JSON object. This is because positions are more important than
    names for rows in SQL.

Cast from JSON
--------------

    Casting to ``BOOLEAN``, ``TINYINT``, ``SMALLINT``, ``INTEGER``,
    ``BIGINT``, ``REAL``, ``DOUBLE`` or ``VARCHAR`` is supported.
    Casting to ``ARRAY`` and ``MAP`` is supported when the element type of
    the array is one of the supported types, or when the key type of the map
    is ``VARCHAR`` and value type of the map is one of the supported types.
    Behaviors of the casts are shown with the examples below::

        SELECT CAST(JSON 'null' AS VARCHAR); -- NULL
        SELECT CAST(JSON '1' AS INTEGER); -- 1
        SELECT CAST(JSON '9223372036854775807' AS BIGINT); -- 9223372036854775807
        SELECT CAST(JSON '"abc"' AS VARCHAR); -- abc
        SELECT CAST(JSON 'true' AS BOOLEAN); -- true
        SELECT CAST(JSON '1.234' AS DOUBLE); -- 1.234
        SELECT CAST(JSON '[1,23,456]' AS ARRAY(INTEGER)); -- [1, 23, 456]
        SELECT CAST(JSON '[1,null,456]' AS ARRAY(INTEGER)); -- [1, NULL, 456]
        SELECT CAST(JSON '[[1,23],[456]]' AS ARRAY(ARRAY(INTEGER))); -- [[1, 23], [456]]
        SELECT CAST(JSON '{"k1":1,"k2":23,"k3":456}' AS MAP(VARCHAR, INTEGER)); -- {k1=1, k2=23, k3=456}
        SELECT CAST(JSON '{"v1":123,"v2":"abc","v3":true}' AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)); -- {v1=123, v2=abc, v3=true}
        SELECT CAST(JSON '[123,"abc",true]' AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)); -- {value1=123, value2=abc, value3=true}

.. note::

    JSON arrays can have mixed element types and JSON maps can have mixed
    value types. This makes it impossible to cast them to SQL arrays and maps in
    some cases. To address this, Presto supports partial casting of arrays and maps::

        SELECT CAST(JSON '[[1, 23], 456]' AS ARRAY(JSON)); -- [JSON '[1,23]', JSON '456']
        SELECT CAST(JSON '{"k1": [1, 23], "k2": 456}' AS MAP(VARCHAR, JSON)); -- {k1 = JSON '[1,23]', k2 = JSON '456'}
        SELECT CAST(JSON '[null]' AS ARRAY(JSON)); -- [JSON 'null']

.. note:: When casting from ``JSON`` to ``ROW``, both JSON array and JSON object are supported.

JSON Functions
--------------
.. function:: is_json_scalar(json) -> boolean

    Determine if ``json`` is a scalar (i.e. a JSON number, a JSON string, ``true``, ``false`` or ``null``)::

        SELECT is_json_scalar('1'); -- true
        SELECT is_json_scalar('[1, 2, 3]'); -- false

.. function:: json_array_contains(json, value) -> boolean

    Determine if ``value`` exists in ``json`` (a string containing a JSON array)::

        SELECT json_array_contains('[1, 2, 3]', 2);

.. function:: json_array_get(json_array, index) -> json

   .. warning::

       The semantics of this function are broken. If the extracted element
       is a string, it will be converted into an invalid ``JSON`` value that
       is not properly quoted (the value will not be surrounded by quotes
       and any interior quotes will not be escaped).

       We recommend against using this function. It cannot be fixed without
       impacting existing usages and may be removed in a future release.

   Returns the element at the specified index into the ``json_array``.
   The index is zero-based::

        SELECT json_array_get('["a", [3, 9], "c"]', 0); -- JSON 'a' (invalid JSON)
        SELECT json_array_get('["a", [3, 9], "c"]', 1); -- JSON '[3,9]'

   This function also supports negative indexes for fetching element indexed
   from the end of an array::

        SELECT json_array_get('["c", [3, 9], "a"]', -1); -- JSON 'a' (invalid JSON)
        SELECT json_array_get('["c", [3, 9], "a"]', -2); -- JSON '[3,9]'

   If the element at the specified index doesn't exist, the function returns null::

        SELECT json_array_get('[]', 0); -- null
        SELECT json_array_get('["a", "b", "c"]', 10); -- null
        SELECT json_array_get('["c", "b", "a"]', -10); -- null

.. function:: json_array_length(json) -> bigint

    Returns the array length of ``json`` (a string containing a JSON array)::

        SELECT json_array_length('[1, 2, 3]');

.. function:: json_extract(json, json_path) -> json

    Evaluates the `JSONPath`_-like expression ``json_path`` on ``json``
    (a string containing JSON) and returns the result as a JSON string::

        SELECT json_extract(json, '$.store.book');

    .. _JSONPath: http://goessner.net/articles/JsonPath/

.. function:: json_extract_scalar(json, json_path) -> varchar

    Like :func:`json_extract`, but returns the result value as a string (as opposed
    to being encoded as JSON). The value referenced by ``json_path`` must be a
    scalar (boolean, number or string)::

        SELECT json_extract_scalar('[1, 2, 3]', '$[2]');
        SELECT json_extract_scalar(json, '$.store.book[0].author');

.. function:: json_format(json) -> varchar

    Returns the JSON text serialized from the input JSON value.
    This is inverse function to :func:`json_parse`::

        SELECT json_format(JSON '[1, 2, 3]'); -- '[1,2,3]'
        SELECT json_format(JSON '"a"'); -- '"a"'

.. note::

    :func:`json_format` and ``CAST(json AS VARCHAR)`` have completely
    different semantics.

    :func:`json_format` serializes the input JSON value to JSON text conforming to
    :rfc:`7159`. The JSON value can be a JSON object, a JSON array, a JSON string,
    a JSON number, ``true``, ``false`` or ``null``::

        SELECT json_format(JSON '{"a": 1, "b": 2}'); -- '{"a":1,"b":2}'
        SELECT json_format(JSON '[1, 2, 3]'); -- '[1,2,3]'
        SELECT json_format(JSON '"abc"'); -- '"abc"'
        SELECT json_format(JSON '42'); -- '42'
        SELECT json_format(JSON 'true'); -- 'true'
        SELECT json_format(JSON 'null'); -- 'null'

    ``CAST(json AS VARCHAR)`` casts the JSON value to the corresponding SQL VARCHAR value.
    For JSON string, JSON number, ``true``, ``false`` or ``null``, the cast
    behavior is same as the corresponding SQL type. JSON object and JSON array
    cannot be cast to VARCHAR::

        SELECT CAST(JSON '{"a": 1, "b": 2}' AS VARCHAR); -- ERROR!
        SELECT CAST(JSON '[1, 2, 3]' AS VARCHAR); -- ERROR!
        SELECT CAST(JSON '"abc"' AS VARCHAR); -- 'abc'; Note the double quote is gone
        SELECT CAST(JSON '42' AS VARCHAR); -- '42'
        SELECT CAST(JSON 'true' AS VARCHAR); -- 'true'
        SELECT CAST(JSON 'null' AS VARCHAR); -- NULL

.. function:: json_parse(string) -> json

    Returns the JSON value deserialized from the input JSON text.
    This is inverse function to :func:`json_format`::

        SELECT json_parse('[1, 2, 3]'); -- JSON '[1,2,3]'
        SELECT json_parse('"abc"'); -- JSON '"abc"'

.. note::

    :func:`json_parse` and ``CAST(string AS JSON)`` have completely
    different semantics.

    :func:`json_parse` expects a JSON text conforming to :rfc:`7159`, and returns
    the JSON value deserialized from the JSON text.
    The JSON value can be a JSON object, a JSON array, a JSON string, a JSON number,
    ``true``, ``false`` or ``null``::

        SELECT json_parse('not_json'); -- ERROR!
        SELECT json_parse('["a": 1, "b": 2]'); -- JSON '["a": 1, "b": 2]'
        SELECT json_parse('[1, 2, 3]'); -- JSON '[1,2,3]'
        SELECT json_parse('"abc"'); -- JSON '"abc"'
        SELECT json_parse('42'); -- JSON '42'
        SELECT json_parse('true'); -- JSON 'true'
        SELECT json_parse('null'); -- JSON 'null'

    ``CAST(string AS JSON)`` takes any VARCHAR value as input, and returns
    a JSON string with its value set to input string::

        SELECT CAST('not_json' AS JSON); -- JSON '"not_json"'
        SELECT CAST('["a": 1, "b": 2]' AS JSON); -- JSON '"[\"a\": 1, \"b\": 2]"'
        SELECT CAST('[1, 2, 3]' AS JSON); -- JSON '"[1, 2, 3]"'
        SELECT CAST('"abc"' AS JSON); -- JSON '"\"abc\""'
        SELECT CAST('42' AS JSON); -- JSON '"42"'
        SELECT CAST('true' AS JSON); -- JSON '"true"'
        SELECT CAST('null' AS JSON); -- JSON '"null"'

.. function:: json_size(json, json_path) -> bigint

    Like :func:`json_extract`, but returns the size of the value.
    For objects or arrays, the size is the number of members,
    and the size of a scalar value is zero::

        SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x'); -- 2
        SELECT json_size('{"x": [1, 2, 3]}', '$.x'); -- 3
        SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x.a'); -- 0
