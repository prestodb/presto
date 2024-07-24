==============
JSON Functions
==============

.. spark:function:: json_object_keys(jsonString) -> array(string)

    Returns all the keys of the outermost JSON object as an array if a valid JSON object is given.  If it is any other valid JSON string, an invalid JSON string or an empty string, the function returns null. ::

        SELECT json_object_keys('{}'); -- []
        SELECT json_object_keys('{"name": "Alice", "age": 5, "id": "001"}'); -- ['name', 'age', 'id']
        SELECT json_object_keys(''); -- NULL
        SELECT json_object_keys(1); -- NULL
        SELECT json_object_keys('"hello"'); -- NULL
