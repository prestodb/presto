============================
JSON Functions and Operators
============================

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
