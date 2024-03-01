============
Release 0.74
============

Bytecode Compiler
-----------------

This version includes new infrastructure for bytecode compilation, and lays the groundwork for future improvements.
There should be no impact in performance or correctness with the new code, but we have added a flag to revert to the
old implementation in case of issues. To do so, add ``compiler.new-bytecode-generator-enabled=false`` to
``etc/config.properties`` in the coordinator and workers.

Hive Storage Format
-------------------

The storage format to use when writing data to Hive can now be configured via the ``hive.storage-format`` option
in your Hive catalog properties file. Valid options are ``RCBINARY``, ``RCTEXT``, ``SEQUENCEFILE`` and ``TEXTFILE``.
The default format if the property is not set is ``RCBINARY``.

General Changes
---------------

* Show column comments in ``DESCRIBE``
* Add :func:`try_cast` which works like :func:`cast` but returns ``null`` if the cast fails
* ``nullif`` now correctly returns a value with the type of the first argument
* Fix an issue with :func:`timezone_hour` returning results in milliseconds instead of hours
* Show a proper error message when analyzing queries with non-equijoin clauses
* Improve "too many failures" error message when coordinator can't talk to workers
* Minor optimization of :func:`json_size` function
* Improve feature normalization algorithm for machine learning functions
* Add exponential back-off to the S3 FileSystem retry logic
* Improve CPU efficiency of semi-joins
