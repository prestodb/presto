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

Notice that casting a JSON text to VARCHAR does not turn the JSON text into
a plain string as is. Instead, it returns the VARCHAR string represented by
the JSON text. This string is different from the JSON text because it has
special characters unescaped and double quotes removed.

JSON Functions
--------------

.. function:: json_extract_scalar(json, json_path) -> varchar

    Evaluates the `JSONPath`_-like expression ``json_path`` on ``json``
    (a string containing JSON) and returns the result as a string. The
    value referenced by ``json_path`` must be a scalar (boolean, number
    or string)::

        SELECT json_extract_scalar('[1, 2, 3]', '$[2]');
        SELECT json_extract_scalar(json, '$.store.book[0].author');

    .. _JSONPath: http://goessner.net/articles/JsonPath/

.. function:: is_json_scalar(json) -> boolean

    Determine if ``json`` is a scalar (i.e. a JSON number, a JSON string,
    ``true``, ``false`` or ``null``)::

        SELECT is_json_scalar('1'); *-- true*
        SELECT is_json_scalar('[1, 2, 3]'); *-- false*

.. function:: json_array_length(json) -> bigint

    Returns the array length of ``json`` (a string containing a JSON
    array). Returns NULL if ``json`` is not an array::

        SELECT json_array_length('[1, 2, 3]');

.. function:: json_array_contains(json, value) -> boolean

    Determine if ``value`` exists in ``json`` (a string containing a JSON
    array). ``value`` could be a boolean, bigint, double, or varchar.
    Returns NULL if ``json`` is not an array::

        SELECT json_array_contains('[1, 2, 3]', 2);

============
JSON Vectors
============

There are a number of Presto JSON functions expecting JSON-typed inputs or
returning JSON-typed outputs. Hence, developers who use the Velox library may
need to work with JSON-typed vectors. In the Velox internal implementation,
the JSON type inherits the VARCHAR type, so the manipulations of these vectors
are similar. To create a JSON-typed vector, one can use
``BaseVector::create(JSON(), size, pool)`` that creates a flat vector of
StringViews, i.e. FlatVector<StringView>. Reading and writing to a JSON-typed
vector are the same as those for VARCHAR vectors, e.g., via
VectorReader<StringView> and StringWriter<>.
