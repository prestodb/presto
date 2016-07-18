==============
JSON Functions
==============

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
