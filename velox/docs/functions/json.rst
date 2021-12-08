==============
JSON Functions
==============

.. function:: json_extract_scalar(json, json_path) -> varchar

    Evaluates the `JSONPath`_-like expression ``json_path`` on ``json``
    (a string containing JSON) and returns the result as a string. The
    value referenced by ``json_path`` must be a scalar (boolean, number
    or string)::

        SELECT json_extract_scalar('[1, 2, 3]', '$[2]');
        SELECT json_extract_scalar(json, '$.store.book[0].author');

    .. _JSONPath: http://goessner.net/articles/JsonPath/
