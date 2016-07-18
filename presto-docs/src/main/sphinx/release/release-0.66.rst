============
Release 0.66
============

Type System
-----------

In this release we have replaced the existing simple fixed type system
with a fully extensible type system and have added several new types.
We have also expanded the function system to support custom
arithmetic, comparison and cast operators. For example, the new date/time
types include an operator for adding an ``INTERVAL`` to a ``TIMESTAMP``.

Existing functions have been updated to operate on and return the
newly added types.  For example, the ANSI color functions now operate
on a ``COLOR`` type, and the date/time functions operate on standard
SQL date/time types (described below).

Finally, plugins can now provide custom types and operators in addition
to connectors and functions. This feature is highly experimental, so expect
the interfaces to change over the next few releases.  Also, since in SQL
there is only one namespace for types, you should be careful to make names
for custom types unique as we will add other common SQL types to Presto
in the near future.

Date/Time Types
---------------

Presto now supports all standard SQL date/time types:
``DATE``, ``TIME``, ``TIMESTAMP`` and ``INTERVAL``.
All of the date/time functions and language constructs now operate on these
types instead of ``BIGINT`` and perform temporal calculations correctly.
This was previously broken due to, for example, not being able to detect
whether an argument was a ``DATE`` or a ``TIMESTAMP``.
This change comes at the cost of breaking existing queries that perform
arithmetic operations directly on the ``BIGINT`` value returned from
the date/time functions.

As part of this work, we have also added the :func:`date_trunc` function
which is convenient for grouping data by a time span. For example, you
can perform an aggregation by hour::

    SELECT date_trunc('hour', timestamp_column), count(*)
    FROM ...
    GROUP BY 1

Time Zones
~~~~~~~~~~

This release has full support for time zone rules, which are needed to
perform date/time calculations correctly. Typically, the session time
zone is used for temporal calculations. This is the time zone of the
client computer that submits the query, if available. Otherwise, it is
the time zone of the server running the Presto coordinator.

Queries that operate with time zones that follow daylight saving can
produce unexpected results. For example, if we run the following query
to add 24 hours using in the ``America/Los Angeles`` time zone::

    SELECT date_add('hour', 24, TIMESTAMP '2014-03-08 09:00:00');
    -- 2014-03-09 10:00:00.000

The timestamp appears to only advance 23 hours. This is because on
March 9th clocks in ``America/Los Angeles`` are turned forward 1 hour,
so March 9th only has 23 hours. To advance the day part of the timestamp,
use the ``day`` unit instead::

    SELECT date_add('day', 1, TIMESTAMP '2014-03-08 09:00:00');
    -- 2014-03-09 09:00:00.000

This works because the :func:`date_add` function treats the timestamp as
list of fields, adds the value to the specified field and then rolls any
overflow into the next higher field.

Time zones are also necessary for parsing and printing timestamps.
Queries that use this functionality can also produce unexpected results.
For example, on the same machine::

    SELECT TIMESTAMP '2014-03-09 02:30:00';

The above query causes an error because there was no 2:30 AM on March 9th
in ``America/Los_Angeles`` due to a daylight saving time transition.

In addition to normal ``TIMESTAMP`` values, Presto also supports the
``TIMESTAMP WITH TIME ZONE`` type, where every value has an explicit time zone.
For example, the following query creates a ``TIMESTAMP WITH TIME ZONE``::

    SELECT TIMESTAMP '2014-03-14 09:30:00 Europe/Berlin';
    -- 2014-03-14 09:30:00.000 Europe/Berlin

You can also change the time zone of an existing timestamp using the
``AT TIME ZONE`` clause::

   SELECT TIMESTAMP '2014-03-14 09:30:00 Europe/Berlin'
        AT TIME ZONE 'America/Los_Angeles';
   -- 2014-03-14 01:30:00.000 America/Los_Angeles

Both timestamps represent the same instant in time;
they differ only in the time zone used to print them.

The time zone of the session can be set on a per-query basis using the
``X-Presto-Time-Zone`` HTTP header, or via the
``PrestoConnection.setTimeZoneId(String)`` method in the JDBC driver.

Localization
~~~~~~~~~~~~

In addition to time zones, the language of the user is important when
parsing and printing date/time types. This release adds localization
support to the Presto engine and functions that require it:
:func:`date_format` and :func:`date_parse`.
For example, if we set the language to Spanish::

    SELECT date_format(TIMESTAMP '2001-01-09 09:04', '%M'); -- enero

If we set the language to Japanese::

    SELECT date_format(TIMESTAMP '2001-01-09 09:04', '%M'); -- 1月

The language of the session can be set on a per-query basis using the
``X-Presto-Language`` HTTP header, or via the
``PrestoConnection.setLocale(Locale)`` method in the JDBC driver.

Optimizations
-------------

* We have upgraded the Hive connector to Hive 0.12 which includes
  performance improvements for RCFile.

* ``GROUP BY`` and ``JOIN`` operators are now compiled to byte code
  and are significantly faster.

* Reduced memory usage of ``GROUP BY`` and ``SELECT DISTINCT``,
  which previously required several megabytes of memory
  per operator, even when the number of groups was small.

* The planner now optimizes function call arguments. This should improve
  the performance of queries that contain complex expressions.

* Fixed a performance regression in the HTTP client. The recent HTTP client
  upgrade was using inadvertently GZIP compression and has a bug in the
  buffer management resulting in high CPU usage.

SPI changes
-----------

In this release we have made a number of backward incompatible changes to the SPI:

* Added ``Type`` and related interfaces
* ``ConnectorType`` in metadata has been replaced with ``Type``
* Renamed ``TableHandle`` to ``ConnectorTableHandle``
* Renamed ``ColumnHandle`` to ``ConnectorColumnHandle``
* Renamed ``Partition`` to ``ConnectorPartition``
* Renamed ``PartitionResult`` to ``ConnectorPartitionResult``
* Renamed ``Split`` to ``ConnectorSplit``
* Renamed ``SplitSource`` to ``ConnectorSplitSource``
* Added a ``ConnectorSession`` parameter to most ``ConnectorMetadata`` methods
* Removed most ``canHandle`` methods

General Bug Fixes
-----------------

* Fixed CLI hang after using ``USE CATALOG`` or ``USE SCHEMA``
* Implicit coercions in aggregations now work as expected
* Nulls in expressions work as expected
* Fixed memory leak in compiler
* Fixed accounting bug in task memory usage
* Fixed resource leak caused by abandoned queries
* Fail queries immediately on unrecoverable data transport errors

Hive Bug Fixes
--------------

* Fixed parsing of timestamps in the Hive RCFile Text SerDe (``ColumnarSerDe``)
  by adding configuration to set the time zone originally used when writing data

Cassandra Bug Fixes
-------------------

* Auto-reconnect if Cassandra session dies
* Format collection types as JSON
