======================
Presto C++ Limitations
======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1


General Limitations
===================

The C++ evaluation engine has a number of limitations:

* Not all built-in functions are implemented in C++. Attempting to use unimplemented functions results in a query failure. For supported functions, see `Function Coverage <https://facebookincubator.github.io/velox/functions/presto/coverage.html>`_.

* Not all built-in types are implemented in C++. Attempting to use unimplemented types will result in a query failure.

  * All basic and structured types in :doc:`../language/types` are supported, except for ``CHAR``, ``TIME``, and ``TIME WITH TIMEZONE``. These are subsumed by ``VARCHAR``, ``TIMESTAMP`` and ``TIMESTAMP WITH TIMEZONE``.

  * Presto C++ only supports unlimited length ``VARCHAR``, and does not honor the length ``n`` in ``varchar[n]``.

  * The following types are not supported: ``IPADDRESS``, ``IPPREFIX``, ``KHYPERLOGLOG``, ``P4HYPERLOGLOG``, ``QDIGEST``, ``TDIGEST``, ``GEOMETRY``, ``BINGTILE``.

* Certain parts of the plugin SPI are not used by the C++ evaluation engine. In particular, C++ workers will not load any plugin in the plugins directory, and certain plugin types are either partially or completely unsupported.

  * ``PageSourceProvider``, ``RecordSetProvider``, and ``PageSinkProvider`` do not work in the C++ evaluation engine.

  * User-supplied functions, types, parametric types and block encodings are not supported.

  * The event listener plugin does not work at the split level.

  * User-defined functions do not work in the same way, see `Remote Function Execution <features.html#remote-function-execution>`_.

* Memory management works differently in the C++ evaluation engine. In particular:

  * The OOM killer is not supported.
  * The reserved pool is not supported.
  * In general, queries may use more memory than they are allowed to through memory arbitration. See `Memory Management <https://facebookincubator.github.io/velox/develop/memory.html>`_.


Functions
=========


Aggregate Functions
-------------------

reduce_agg
^^^^^^^^^^
In C++ based Presto, ``reduce_agg`` is not permitted to return ``null`` in either the 
``inputFunction`` or the ``combineFunction``. In Presto (Java), this is permitted 
but undefined behavior. For more information about ``reduce_agg`` in Presto, 
see `reduce_agg <../functions/aggregate.html#reduce_agg>`_. 

reduce lambda
^^^^^^^^^^^^^
For the reduce lambda function, the array size is controlled by the session property 
``native_expression_max_array_size_in_reduce``, as it is inefficient to support such 
cases for arbitrarily large arrays. This property is set at ``100K``. Queries that 
fail due to this limit must be revised to meet this limit.


Array Functions
---------------

Array sort with lambda comparator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``Case`` is not supported for the lambda comparator. Use ``If`` Instead. The following 
example is not supported in Presto C++:

.. code-block:: sql

   (x, y) ->
   CASE
   WHEN x.event_time < y.event_time THEN
     -1
   WHEN x.event_time > y.event_time THEN
     1
     ELSE 0
   END

To work with Presto C++, the best option is to use transform lambda whenever possible. 
For example:

.. code-block:: sql

   (x) -> x.event_time

Or, rewrite using ``if`` as in the following example:

.. code-block:: sql

   (x, y) -> IF (x.event_time < y.event_time, -1, 
   IF (x.event_time > y.event_time, 1, 0))

When using ``If``, follow these rules when using a lambda in array sort:

* The lambda should use ``if else``. Case is not supported.
* The lambda should return ``1``, ``0``, ``-1``. Cover all the cases.
* The lambda should use the same expression when doing the comparison. 
  For example, in the above case ``event_time`` is used for comparison throughout the lambda. 
  If we rewrote the expression as following, where ``x`` and ``y`` have different fields, it will fail: 
  ``(x, y) -> if (x.event_time < y.event_start_time, -1, if (x.event_time > y.event_start_time, 1, 0))``
* Any additional nesting other than the two ``if`` uses shown above will fail.

``Array_sort`` can support any transformation lambda that returns a comparable type. 
This example is not supported in Presto C++:

.. code-block:: sql

   "array_sort"("map_values"(m), (a, b) -> (
                CASE WHEN (a[1] [2] > b[1] [2]) THEN 1 
                     WHEN (a[1] [2] < b[1] [2]) THEN -1 
                     WHEN (a[1] [2] = b[1] [2]) THEN 
                          IF((a[3] > b[3]), 1, -1) END)

To run in Presto C++, rewrite the query as shown in this example:

.. code-block:: sql

   "array_sort"("map_values"(m), (a) -> ROW(a[1][2], a[3]))
   

Casting
-------

Casting of Unicode strings to digits is not supported. The following example is not supported in Presto C++:

.. code-block:: sql

   CAST ('Ⅶ' as integer)


Date and Time Functions
-----------------------
The maximum date range supported by ``from_unixtime`` is between (292 Million BCE, 292 Million CE). 
The exact values corresponding to this are [292,275,055-05-16 08:54:06.192 BC, +292,278,994-08-17 00:12:55.807 CE], 
corresponding to a UNIX time between [-9223372036854775, 9223372036854775]. 

Presto and Presto C++ both support the same range but Presto queries succeed because Presto silently 
truncates. Presto C++ throws an error if the values exceed this range. 


Geospatial Differences
----------------------
There are cosmetic representation changes as well as numerical precision differences. 
Some of these differences result in different output for spatial predicates such 
as ST_Intersects. Differences include:

* Equivalent but different representations for geometries. Polygons may have their rings 
  rotated, EMPTY geometries may be of a different type, MULTI-types and 
  GEOMETRYCOLLECTIONs may have their elements in a different order. In general, 
  WKTs/WKBs may be different.
* Numerical precision: Differences in numerical techniques may result in different 
  coordinate values, and also different results for predicates (ST_Relates and children, 
  including ST_Contains, ST_Crosses, ST_Disjoint, ST_Equals, ST_Intersects, 
  ST_Overlaps, ST_Relate, ST_Touches, ST_Within).
* ST_IsSimple, ST_IsValid, simplify_geometry and geometry_invalid_reason may give different results.


JSON Functions
--------------
``json_extract`` has several topics to consider when rewriting Presto queries to run successfully in Presto C++.

Use of functions in JSON path
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Using functions inside a JSON path is not supported.

To run queries with functions inside a JSON path in Presto C++, rewrite paths to 
use equivalent and often faster UDFs (User-Defined Functions) outside the JSON 
path, improving job portability and efficiency. Aggregates might be necessary. 

Generally, functions should be extracted from the JSON path for better portability.

For example, this Presto query:

.. code-block:: sql

   CAST(JSON_EXTRACT(config, '$.table_name_to_properties.keys()'
     ) AS ARRAY(ARRAY(VARCHAR)))

can be revised to work in both Presto and Presto C++ as the following: 

.. code-block:: sql

   map_keys(JSON_EXTRACT( config, '$.table_name_to_properties') )

Use of expressions in JSON path
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Paths containing filter expressions are not supported.

To run such queries in Presto C++, revise the query to do the filtering as a 
part of the SQL expression query, rather than in the JSON path.

For example, consider this Presto query: 

.. code-block:: sql

   JSON_EXTRACT(config, '$.store.book[?(@.price > 10)]')

The same query rewritten to run in Presto C++: 

.. code-block:: sql

   filter(
   CAST(json_extract(data, '$.store.book') AS ARRAY<JSON>),
   x -> CAST(json_extract_scalar(x.value, '$.price') AS DOUBLE) > 10)
   )

Erroring on Invalid JSON
^^^^^^^^^^^^^^^^^^^^^^^^
Presto can successfully run ``json_extract`` on certain invalid JSON, but Presto C++ 
always fails. Extracting data from invalid JSON is indeterminate and relying on 
that behavior can have unintended consequences. 

Because Presto C++ takes the safe approach to always throw an error on invalid 
JSON, wrap calls in a try to ensure the query succeeds and validate that the 
results correspond to your expectations. 

Canonicalization
^^^^^^^^^^^^^^^^
Presto ``json_extract`` can return `JSON that is not canonicalized <https://github.com/prestodb/presto/issues/24563#issue-2852506643>`_. 
``json_extract`` has been rewritten in Presto C++ to always return canonical JSON.


Regex Functions
---------------

Unsupported Cases 
^^^^^^^^^^^^^^^^^
Presto C++ uses `RE2 <https://github.com/google/re2>`_, a widely adopted modern regular 
expression parsing library. 

Presto uses `JONI <https://github.com/jruby/joni>`_, a deprecated port of Oniguruma (ONIG). 

While both frameworks support almost all regular expression syntaxes, RE2 differs from 
JONI and PCRE in certain cases. The following are not supported in Presto C++ but are supported in Presto:

* before text matching (?=re)
* before text not matching (?!re)
* after text matching (?<=re) 
* after text not matching (?<!re)

Presto queries using these, and 
unsupported regular expressions listed in `Syntax <https://github.com/google/re2/wiki/syntax>`_, 
must be rewritten to run in Presto C++. See `Syntax <https://github.com/google/re2/wiki/syntax>`_ 
for a full list of unsupported regular expressions in RE2 and 
`Caveats <https://swtch.com/~rsc/regexp/regexp3.html#caveats>`_ for an explanation of 
why RE2 skips certain syntax in Perl. 

Regex Compilation Limit in Velox
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Because Regex compilation is CPU intensive, unbounded compilation can cause problems. 
The number of regular expressions that can be dynamically compiled for a query is limited 
to 250 to keep the overall shared cluster environment healthy. 

If this limit is reached, rewrite the query to use fewer compiled regular expressions. 

In this example the regex can change based on the ``test_name`` column value, which could exceed the 250 limit:

.. code-block:: sql

   code_location_path LIKE '%' || test_name || '%' 

Revise the query as follows to avoid this limit: 

.. code-block:: sql

   strpos(code_location, test_name) > 0


Time and Time with Time Zone
----------------------------

IANA Named Timezones Support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Support for IANA named time zones - for example, `Europe/London`, `UTC`, `America/New_York`, 
`Asia/Kolkata` - in ``TIME`` and ``TIME WITH TIME ZONE`` was removed from Presto C++ 
to align with the SQL standard. Only fixed-offset time zones such as `+02:00` are 
now supported for these types.

Named time zones may still work when the Presto coordinator handles the query.

To run queries involving ``TIME`` and ``TIME WITH TIME ZONE``, migrate to fixed-offset 
time zones as soon as possible.

These queries will fail in Presto C++, but may still work in Presto:

.. code-block:: sql

   cast('14:00:01 UTC' as TIME WITH TIME ZONE)
   cast('14:00:01 Europe/Paris' as TIME WITH TIME ZONE)
   cast('14:00:01 America/New_York' as TIME WITH TIME ZONE)
   cast('14:00:01 Asia/Kolkata' as TIME WITH TIME ZONE)

These queries using fixed offsets will run successfully in Presto C++:

.. code-block:: sql

   cast('14:00:01 +00:00' as TIME WITH TIME ZONE)
   cast('14:00:01 +05:30' as TIME WITH TIME ZONE)

Casting from TIMESTAMP to TIME
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In Presto, the result of CAST(TIMESTAMP AS TIME) or CAST(TIMESTAMP AS TIME WITH TIME ZONE) 
would change based on the session property ``legacy_timestamp`` (true by default) when 
applied to the user's time zone. In Presto C++ for ``TIME`` and ``TIME WITH TIME ZONE``, 
the behavior is equivalent to the property being `false`.

Note: ``TIMESTAMP`` behavior in Presto and Presto C++ is unchanged.

For examples, consider the following queries and their responses when run in Presto: 

.. code-block:: sql

   -- Default behavior with legacy_timestamp=true:
   -- Session Timezone - America/Los_Angeles

   -- DST Active Dates 
   select cast(TIMESTAMP '2023-08-05 10:15:00.000' as TIME);
   -- Returns: 09:15:00.000
   select cast(TIMESTAMP '2023-08-05 10:15:00.000' as TIME WITH TIME ZONE);
   -- Returns: 09:15:00.000 America/Los_Angeles
  select cast(TIMESTAMP '2023-08-05 10:15:00.000 America/Los_Angeles' as TIME);
  -- Returns: 09:15:00.000
  select cast(TIMESTAMP '2023-08-05 10:15:00.000 America/Los_Angeles' as TIME WITH TIME ZONE);
  -- Returns: 09:15:00.000

  -- DST Inactive Dates
  select cast(TIMESTAMP '2023-12-05 10:15:00.000' as TIME);
  -- Returns: 10:15:00.000
  select cast(TIMESTAMP '2023-12-05 10:15:00.000' as TIME WITH TIME ZONE);
  -- Returns: 10:15:00.000 America/Los_Angeles
  select cast(TIMESTAMP '2023-08-05 10:15:00.000 America/Los_Angeles' as TIME);
  -- Returns: 10:15:00.000
  select cast(TIMESTAMP '2023-12-05 10:15:00.000 America/Los_Angeles' as TIME WITH TIME ZONE);
 -- 10:15:00.000 America/Los_Angeles

Consider the following queries and their responses when run in Presto C++ (Velox): 

.. code-block:: sql

   -- New Expected behavior similar to what currently exists if legacy_timestamp=false:
   -- Session Timezone - America/Los_Angeles


   -- DST Active Dates 
   select cast(TIMESTAMP '2023-08-05 10:15:00.000' as TIME);
   -- Returns: 10:15:00.000
   select cast(TIMESTAMP '2023-08-05 10:15:00.000' as TIME WITH TIME ZONE);
   -- Returns: 10:15:00.000 -07:00
   select cast(TIMESTAMP '2023-08-05 10:15:00.000 America/Los_Angeles' as TIME);
   -- Returns: 10:15:00.000
   select cast(TIMESTAMP '2023-08-05 10:15:00.000 America/Los_Angeles' as TIME WITH TIME ZONE);
   -- Returns: 10:15:00.000 -07:00

   -- DST Inactive Dates
   select cast(TIMESTAMP '2023-12-05 10:15:00.000' as TIME);
   -- Returns: 10:15:00.000
   select cast(TIMESTAMP '2023-12-05 10:15:00.000' as TIME WITH TIME ZONE);
   -- Returns: 10:15:00.000 -08:00
   select cast(TIMESTAMP '2023-08-05 10:15:00.000 America/Los_Angeles' as TIME);
   -- Returns: 10:15:00.000
   select cast(TIMESTAMP '2023-12-05 10:15:00.000 America/Los_Angeles' as TIME WITH TIME ZONE);
   -- Returns: 10:15:00.000 -08:00

Note: ``TIMESTAMP`` supports named time zones, unlike ``TIME`` and ``TIME WITH TIME ZONE``.

DST Implications
^^^^^^^^^^^^^^^^
Because IANA zones are not supported for ``TIME``, Presto C++ does not manage DST transitions. 
All time interpretation is strictly in the provided offset, not local civil time.

For example, ``14:00:00 +02:00`` always means 14:00 at a +02:00 fixed offset, regardless 
of DST changes that might apply under an IANA zone.

Recommendations
^^^^^^^^^^^^^^^
* Use fixed-offset time zones like +02:00 with ``TIME`` and ``TIME WITH TIME ZONE``.
* Do not use IANA time zone names for ``TIME`` and ``TIME WITH TIME ZONE``. 
* Confirm that your Presto C++ usage does not depend on legacy timestamp behavior. If your workload 
  depends on legacy ``TIME`` behavior, including support of IANA timezones, handle this outside 
  Presto or reach out so that we can discuss alternative solutions.
* Test: Try your most critical workflows with these settings.


URL Functions
-------------

Presto and Presto C++ implement different URL function specifications which can lead to 
some URL function mismatches. Presto C++ implements `RFC-3986 <https://datatracker.ietf.org/doc/html/rfc3986>`_ whereas Presto 
implements `RFC-2396 <https://datatracker.ietf.org/doc/html/rfc2396>`_. This can lead to subtle differences as presented in 
`this issue <https://github.com/facebookincubator/velox/issues/14204>`_.

Window Functions
----------------

Aggregate window functions do not support ``IGNORE NULLS``, returning the following error message:

``!ignoreNulls Aggregate window functions do not support IGNORE NULLS.``

For Presto C++, remove the ``IGNORE NULLS`` clause. This clause is only defined for value functions 
and does not apply to aggregate window functions. In Presto the results obtained with and without 
the clause are similar, Presto C++ includes this clause whereas Presto just warns.