# Presto 0.2

## New functions and operators

* `STRPOS(string, substring)` returns the starting position (one-based) of `substring` within `string`, or zero if not found
* `REPLACE(string, search[, replace])` replaces all instances of `search` with `replace` in `string`
* `str1 || str2` concatenates `str1` and `str2`

## Self joins

Joining a table with itself is now supported. This was caused by a bug in the planner that did not allow a table to appear more than once in a plan.

## Prism integration

We now have real Prism integration with table link support.  Prism table links are referenced as follows:

    select * from table@namespace;

## Hive compatibility

Unpartitioned tables are now supported. They will appear as having a single default partition.

Tables or partitions utilizing `SymlinkTextImportFormat` are now supported.

## CLI table completion

Simple tab completion support is available for table names.

## Simple REST query endpoint

Queries can be performed by making a `POST` to the `/v1/execute` resource. The results will be returned as JSON. Example:

    $ curl -d "select 123 a, 'hello' b from dual" http://localhost:8080/v1/execute?pretty
    {
      "columns" : [ {
        "name" : "a",
        "type" : "bigint"
      }, {
        "name" : "b",
        "type" : "varchar"
      } ],
      "data" : [ [ 123, "hello" ] ]
    }


## Hive metadata caching

Hive metadata caching is utilized more effectively. Query planning time should now be significantly reduced after caching. Metadata is cached by default for one hour, thus metadata changes will be visible after that amount of time.
