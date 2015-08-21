=============
Release 0.116
=============

Cast between JSON and VARCHAR
-----------------------------

Casts of both directions between JSON and VARCHAR have been removed. If you
have such casts in your scripts or views, they will fail with a message when
you move to release 0.116. To get the semantics of the current casts, use:

* `JSON_PARSE(x)` instead of `CAST(x as JSON)`
* `JSON_FORMAT(x)` instead of `CAST(x as VARCHAR)`

In a future release, we intend to reintroduce casts between JSON and VARCHAR
along with other casts involving JSON. The semantics of the new JSON and
VARCHAR cast will be consistent with the other casts being introduced. But it
will be different from the semantics in 0.115 and before. When that comes,
cast between JSON and VARCHAR in old scripts and views will produce unexpected
result.

General Changes
---------------

* Add :func:`multimap_agg` function.
