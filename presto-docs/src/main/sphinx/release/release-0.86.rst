============
Release 0.86
============

General Changes
---------------

* Add support for inequality ``INNER JOIN`` when each term of the condition refers to only one side of the join.
* Add :func:`ntile` function.
* Add :func:`map` function to create a map from arrays of keys and values.
* Add :func:`min_by` aggregation function.
* Add support for concatenating arrays with the ``||`` operator.
* Add support for ``=`` and ``!=`` to ``JSON`` type.
* Improve error message when ``DISTINCT`` is applied to types that are not comparable.
* Perform type validation for ``IN`` expression where the right-hand side is a subquery expression.
* Improve error message when ``ORDER BY ... LIMIT`` query exceeds its maximum memory allocation.
* Improve error message when types that are not orderable are used in an ``ORDER BY`` clause.
* Improve error message when the types of the columns for subqueries of a ``UNION`` query don't match.
* Fix a regression where queries could be expired too soon on a highly loaded cluster.
* Fix scheduling issue for queries involving tables from information_schema, which could result in
  inconsistent metadata.
* Fix an issue with :func:`min_by` and :func:`max_by` that could result in an error when used with
  a variable-length type (e.g., ``VARCHAR``) in a ``GROUP BY`` query.
* Fix rendering of array attributes in JMX connector.
* Input rows/bytes are now tracked properly for ``JOIN`` queries.
* Fix case-sensitivity issue when resolving names of constant table expressions.
* Fix unnesting arrays and maps that contain the ``ROW`` type.
