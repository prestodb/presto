============================
JSON Functions and Operators
============================

Cast to JSON
------------

    Casting from ``BOOLEAN``, ``TINYINT``, ``SMALLINT``, ``INTEGER``,
    ``BIGINT``, ``REAL``, ``DOUBLE`` or ``VARCHAR`` is supported.
    Casting from ``ARRAY`` and ``MAP`` is supported when the element type of
    the array is one of the supported types, or when the key type of the map
    is ``VARCHAR`` and value type of the map is one of the supported types.
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
        SELECT CAST(MAP(ARRAY['k1', 'k2', 'k3'], ARRAY[1, 23, 456]) AS JSON); -- JSON '{"k1":1,"k2":23,"k3":456}'

    Note that casting from NULL to ``JSON`` is not straightforward. Casting
    from a standalone ``NULL`` will produce a SQL ``NULL`` instead of
    ``JSON 'null'``. However, when casting from arrays or map containing
    ``NULL``\s, the produced ``JSON`` will have ``null``\s in it.

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

    JSON arrays can have mixed element types and JSON maps can have mixed
    value types. This makes it impossible to cast them to SQL arrays and maps in
    some cases. To address this, Presto supports partial casting of arrays and maps::

        SELECT CAST(JSON '[[1, 23], 456]' AS ARRAY(JSON)); -- [JSON '[1,23]', JSON '456']
        SELECT CAST(JSON '{"k1": [1, 23], "k2": 456}' AS MAP(VARCHAR, JSON)); -- {k1 = JSON '[1,23]', k2 = JSON '456'}
        SELECT CAST(JSON '[null]' AS ARRAY(JSON)); -- [JSON 'null']

JSON Functions
--------------

.. function:: json_array_contains(json, value) -> boolean

    Determine if ``value`` exists in ``json`` (a string containing a JSON array)::

        SELECT json_array_contains('[1, 2, 3]', 2);

.. function:: json_array_get(json_array, index) -> varchar

   Returns the element at the specified index into the ``json_array``.
   The index is zero-based::

        SELECT json_array_get('["a", "b", "c"]', 0); -- 'a'
        SELECT json_array_get('["a", "b", "c"]', 1); -- 'b'

   This function also supports negative indexes for fetching element indexed
   from the end of an array::

        SELECT json_array_get('["c", "b", "a"]', -1); -- 'a'
        SELECT json_array_get('["c", "b", "a"]', -2); -- 'b'

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

    Returns ``json`` as a string::

        SELECT json_format(JSON '[1, 2, 3]'); -- '[1,2,3]'
        SELECT json_format(JSON '"a"'); -- '"a"'

.. function:: json_parse(string) -> json

    Parse ``string`` as a json::

        SELECT json_parse('[1, 2, 3]'); -- JSON '[1,2,3]'
        SELECT json_parse('"a"'); -- JSON '"a"'

.. function:: json_size(json, json_path) -> bigint

    Like :func:`json_extract`, but returns the size of the value.
    For objects or arrays, the size is the number of members,
    and the size of a scalar value is zero::

        SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x'); -- 2
        SELECT json_size('{"x": [1, 2, 3]}', '$.x'); -- 3
        SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x.a'); -- 0
