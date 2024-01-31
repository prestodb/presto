==============
JSON Functions
==============

JSON Format
-----------
JSON is a language-independent data format that represents data as
human-readable text. A JSON text can represent a number, a boolean, a
string, an array, an object, or a null, with slightly different grammar.
For instance, a JSON text representing a string must escape all characters
and enclose the string in double quotes, such as ``"123\n"``, whereas a JSON
text representing a number does not need to, such as ``123``. A JSON text
representing an array must enclose the array elements in square brackets,
such as ``[1,2,3]``. More detailed grammar can be found in
`this JSON introduction`_.

.. _this JSON introduction: https://www.json.org

Cast to JSON
------------
Casting a value from a supported type to JSON returns a JSON text that
represents this value. Casting from BOOLEAN, TINYINT, SMALLINT, INTEGER,
BIGINT, REAL, DOUBLE, DATE, TIMESTAMP, or VARCHAR is supported. Casting
from ARRAY or ROW is supported when the element type of the array is one
of the supported types, or when every field type of the row is one of the
supported types. Casting from MAP is supported when the key type of the map
is BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, or VARCHAR
and value type of the map is one of the supported types. Additionally,
element types of ARRAY and MAP and field types of ROW are also allowed to
be JSON. Behaviors of the casts are shown with the examples below:

::

    SELECT CAST(NULL AS JSON); -- NULL
    SELECT CAST(1 AS JSON); -- JSON '1'
    SELECT CAST(9223372036854775807 AS JSON); -- JSON '9223372036854775807'
    SELECT CAST('abc' AS JSON); -- JSON '"abc"'
    SELECT CAST(true AS JSON); -- JSON 'true'
    SELECT CAST(1.234 AS JSON); -- JSON '1.234'
    SELECT CAST(-0.00012 AS JSON); -- JSON '-1.2E-4'
    SELECT CAST(10000000.0 AS JSON); -- JSON '1.0E7'
    SELECT CAST(ARRAY[1, 23, 456] AS JSON); -- JSON '[1,23,456]'
    SELECT CAST(ARRAY[1, NULL, 456] AS JSON); -- JSON '[1,null,456]'
    SELECT CAST(ARRAY[ARRAY[1, 23], ARRAY[456]] AS JSON); -- JSON '[[1,23],[456]]'
    SELECT CAST(MAP_FROM_ENTRIES(ARRAY[('k1', 1), ('k2', 23), ('k3', 456)]) AS JSON); -- JSON '{"k1":1,"k2":23,"k3":456}'
    SELECT CAST(CAST(ROW(123, 'abc', true, JSON '["a"]') AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN, v4 JSON)) AS JSON); -- JSON '[123,"abc",true,["a"]]'

Notice that casting from NULL to JSON is not straightforward. Casting from
a standalone NULL will produce a SQL NULL instead of JSON 'null'. However,
when casting from arrays or map containing NULLs, the produced JSON will
have nulls in it.

Another thing to be aware of is that when casting from ROW to JSON, the
result is a JSON array rather than a JSON object. This is because positions
are more important than names for rows in SQL.

Also note that casting from REAL or DOUBLE returns the JSON text represented
in standard notation if the magnitude of input value is greater than or equal
to 10 :superscript:`-3` but less than 10 :superscript:`7`, and returns the JSON
text in scientific notation otherwise. The standard and scientific notation
always has the fractional part, such as ``10.0``.

Finally, keep in mind that casting a VARCHAR string to JSON does not directly
turn the original string into JSON type. Instead, it creates a JSON text
representing the original string. This JSON text is different from the original
string since it has special characters escaped and extra double quotes added.

Cast from JSON
--------------
Casting a JSON text to a supported type returns the value represented by this
JSON text. The JSON text must represent a valid value of the type it is casted
to, or an error will be thrown. Casting to BOOLEAN, TINYINT, SMALLINT, INTEGER,
BIGINT, REAL, DOUBLE or VARCHAR is supported. Casting to ARRAY and MAP is
supported when the element type of the array is one of the supported types, or
when the key type of the map is BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT,
REAL, DOUBLE, or VARCHAR and value type of the map is one of the supported types.
When casting from JSON to ROW, both JSON array and JSON object are supported.
Cast from JSON object to ROW uses case insensitive match for the JSON keys.
Behaviors of the casts are shown with the examples below:

::

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
    SELECT CAST(JSON '{"V1":123,"V2":"abc","V3":true}' AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)); -- {v1=123, v2=abc, v3=true}
    SELECT CAST(JSON '[123,"abc",true]' AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)); -- {v1=123, v2=abc, v3=true}

Notice that casting a JSON text to VARCHAR does not turn the JSON text into
a plain string as is. Instead, it returns the VARCHAR string represented by
the JSON text. This string is different from the JSON text because it has
special characters unescaped and double quotes removed.

JSON Functions
--------------

.. function:: is_json_scalar(json) -> boolean

    Determine if ``json`` is a scalar (i.e. a JSON number, a JSON string,
    ``true``, ``false`` or ``null``)::

        SELECT is_json_scalar('1'); *-- true*
        SELECT is_json_scalar('[1, 2, 3]'); *-- false*

.. function:: json_array_contains(json, value) -> boolean

    Determine if ``value`` exists in ``json`` (a string containing a JSON
    array). ``value`` could be a boolean, bigint, double, or varchar.
    Returns NULL if ``json`` is not an array::

        SELECT json_array_contains('[1, 2, 3]', 2);

.. function:: json_array_length(json) -> bigint

    Returns the array length of ``json`` (a string containing a JSON
    array). Returns NULL if ``json`` is not an array::

        SELECT json_array_length('[1, 2, 3]');

.. function:: json_extract(json, json_path) -> json

    Evaluates the `JSONPath`_-like expression ``json_path`` on ``json``
    (a string containing JSON) and returns the result as a JSON string::

        SELECT json_extract(json, '$.store.book');

    Current implementation supports limited subset of JSONPath syntax.

    .. _JSONPath: http://goessner.net/articles/JsonPath/

.. function:: json_extract_scalar(json, json_path) -> varchar

    Evaluates the `JSONPath`_-like expression ``json_path`` on ``json``
    (a string containing JSON) and returns the result as a string. The
    value referenced by ``json_path`` must be a scalar (boolean, number
    or string)::

        SELECT json_extract_scalar('[1, 2, 3]', '$[2]');
        SELECT json_extract_scalar(json, '$.store.book[0].author');

    .. _JSONPath: http://goessner.net/articles/JsonPath/

.. function:: json_format(json) -> varchar

    Serializes the input JSON value to JSON text conforming to `RFC 7159`_.
    The JSON value can be a JSON object, a JSON array, a JSON string, a JSON number, ``true``, ``false`` or ``null``::

        SELECT json_format(JSON '[1, 2, 3]'); -- '[1,2,3]'
        SELECT json_format(JSON '"a"'); -- '"a"'

    .. _RFC 7159: https://datatracker.ietf.org/doc/html/rfc7159.html

.. function:: json_parse(varchar) -> json

    expects a JSON text conforming to `RFC 7159`_, and returns the JSON value deserialized from the JSON text.
    The JSON value can be a JSON object, a JSON array, a JSON string, a JSON number, ``true``, ``false`` or ``null``::

        SELECT json_parse('[1, 2, 3]'); -- JSON '[1,2,3]'
        SELECT json_parse('"abc"'); -- JSON '"abc"'

    .. _RFC 7159: https://datatracker.ietf.org/doc/html/rfc7159.html

.. function:: json_size(json, value) -> bigint

    Returns the size of the ``value``. For ``objects`` or ``arrays``, the size
    is the number of members, and the size of a ``scalar`` value is zero::

        SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x'); -- 2
        SELECT json_size('{"x": [1, 2, 3]}', '$.x'); -- 3
        SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x.a'); -- 0

JSON Vectors
------------

There are a number of Presto JSON functions expecting JSON-typed inputs or
returning JSON-typed outputs. Hence, developers who use the Velox library may
need to work with JSON-typed vectors. In the Velox internal implementation,
the JSON type inherits the VARCHAR type, so the manipulations of these vectors
are similar. To create a JSON-typed vector, one can use
``BaseVector::create(JSON(), size, pool)`` that creates a flat vector of
StringViews, i.e. FlatVector<StringView>. Reading and writing to a JSON-typed
vector are the same as those for VARCHAR vectors, e.g., via
VectorReader<StringView> and StringWriter<>.
