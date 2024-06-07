=================================
Timestamp and Timezone Management
=================================

Concepts
--------

Following ANSI SQL semantics, TIMESTAMP is a data type that represents a
reading of a wall clock and a calendar, e.g, ``2024-04-09 18:25:00``. Note that
a TIMESTAMP does not represent an absolute point in time, as the exact same
wall clock time may be read in different instants in time depending on where
one is situated on Earth. For example, ``2024-04-09 18:25:00`` in California
and in China were perceived at different absolute points in time, about 15
hours apart.  

To represent absolute points in time, SQL defines a TIMESTAMP WITH TIMEZONE
type, which conceptually represents a pair of a wall time and calendar read
(say, ``2024-04-09 18:25:00``), and a timezone (``PDT``, or
``America/Los_Angeles``). With these two values, one can unambiguously
represent an absolute instant in time. 

Naturally, a TIMESTAMP WITH TIMEZONE can be cast into a TIMESTAMP by just
ignoring the timezone and keeping the timestamp wall time, and a TIMESTAMP can
be cast into a TIMESTAMP WITH TIMEZONE by associating a timezone to it. The
timezone can either be explicitly specified by the users, or implicitly taken
from the user system or session information. 

Physical Representation
-----------------------

Representing timestamps in memory as a string or a set of values for year,
month, day, hour, and so on, is inefficient. Therefore, timestamps are usually
stored in a columnar layout as a 64 bit integer representing the number of
seconds elapsed since ``1970-01-01 00:00:00``. Negative values represent time
prior to that. 

However, note that the physical representation of the timestamp is orthogonal
to its logical meaning. For example, the timestamp represented by the ``0``
integer was perceived at different absolute points in time depending on the
observer’s timezone, and does not necessarily imply that it was observed in the
UTC timezone. When a timestamp represents the number of seconds in UTC
specifically (at that exact absolute instant in time), it may be called a *unix
epoch* or *unix time*.

Velox Classes and APIs
----------------------

Velox provides a few classes and APIs to allow developers to store, process,
and convert timestamps across timezones:

**Timestamp:** In Velox, timestamps are represented by the `Timestamp
<https://github.com/facebookincubator/velox/blob/main/velox/type/Timestamp.h>`_
class. The Timestamp class stores two 64 bit integers, one containing the
number of seconds from ``1970-01-01 00:00:00``, and one containing the
nanoseconds offset in that particular second, in order to provide nanosecond
precision. A few more observations:

* While “seconds” can be negative to represent time before ``1970-01-01
  00:00:00``, “nanoseconds” are always positive.

* Although Velox supports nanoseconds precision, engines like Presto and Spark
  may only need milliseconds or microsecond precision.

* The Timestamp class only offers a physical representation of timestamps, but
  does not carry logical information about its timezone. In other words, it
  cannot, by itself, represent an absolute point in time.

**Timezone IDs:** To physically represent timezones, Velox provides the
`TimezoneMap.h <https://github.com/facebookincubator/velox/blob/main/velox/type/tz/TimeZoneMap.h>`_
API. This API provides a 1:1 mapping from each available timezone to a
monotonically increasing integer (a timezone ID), such that this integer can be
used to efficiently represent timezones, preventing the use of inefficient
timezone string names like ``America/Los_Angeles``. Considering there are about
2k valid timezone definitions, 12 bits are enough to represent timezone IDs. 

Timezone IDs in Velox are based on the id map used by Presto and are 
`available here <https://github.com/prestodb/presto/blob/master/presto-common/src/main/resources/com/facebook/presto/common/type/zone-index.properties>`_. 
They are automatically generated using `this script <https://github.com/facebookincubator/velox/blob/main/velox/type/tz/gen_timezone_database.py>`_. 
While timezone IDs are an implementation detail and ideally should not leak
outside of Velox execution, they are exposed if data containing
TimestampWithTimezones are serialized, for example.

**TimestampWithTimezone:** To represent an absolute point in time, Velox provides
`TimestampWithTimezone <https://github.com/facebookincubator/velox/blob/main/velox/functions/prestosql/types/TimestampWithTimeZoneType.h>`_.
This abstraction implements the TIMESTAMP WITH TIMEZONE SQL semantic discussed
above, and is based on Presto’s implementation - therefore only supporting
millisecond-precision.

TimestampWithTimezone physically packs two integers in a single 64 word, using
12 bits for timezone ID, and 52 bits for a millisecond-precision timestamp.

Note that to accelerate timestamp conversion functions, the timestamps stored
in a TimestampWithTimezone are **always relative to UTC** - they are unix epochs.
This means that converting a TimestampWithTimezone across timezones is
efficiently done by just overwriting the 12 bits, and that comparisons can be
done by just comparing the 52 bits relative to timestamp (ignoring the timezone
ID).

However, unpacking/converting a TimestampWithTimezone into an absolute time
definition requires a
`timezone conversion <https://github.com/facebookincubator/velox/blob/main/velox/functions/prestosql/DateTimeFunctions.h#L74-L84>`_. 

Conversions Across Timezones
----------------------------

A common operation required when processing timestamps and timezone is finding
the wall clock and calendar read in a specific timezone given an absolute point
in time described by a wall clock and calendar read in a different timezone.
For example, at the exact point in time when UTC hits ``1970-01-01 00:00:00``,
what was the wall clock read in China?

Timezone conversions are tricky since they are non-linear and depend on
daylight savings time schedules and other local regulations, and these change
over time. To enable such conversions, `IANA <https://www.iana.org/time-zones>`_
periodically publishes a global source of authority database for timezone
conversions, which is periodically pushed to systems using packages like tzdata
for Linux. 

In Velox, Timezone conversions are done using std::chrono. Starting in C++20,
std::chrono `supports conversion of timestamp across timezones <https://en.cppreference.com/w/cpp/chrono/time_zone>`_.
To support older versions of the C++ standard, in Velox we vendor an
implementation of this API at `velox/external/date/ <https://github.com/facebookincubator/velox/tree/main/velox/external/date>`_.
This class handles timezone conversions by leveraging APIs provided by the
operating system, based on the tzdata database installed locally. If systems
happen to have inconsistent or older versions of the tzdata database, Velox’s
conversions may produce inconsistent results. 

On Linux, you can check the tzdata installed in your system by:

.. code-block:: bash

  $ rpm -qa | grep tzdata
  tzdata-2024a-1.fc38.noarch

Timezone conversions are done using special methods in the Timestamp class:
``Timestamp::toGMT()`` and ``Timestamp::toTimezone()``. They can take either a
timezone ID or a date::time_zone pointer. Providing a date::time_zone is
generally more efficient, but std::chrono does not handle time zone offsets
such as ``+09:00``.  Timezone offsets are only supported in the API version
that takes a timezone ID.

Casts
-----

This section describes examples of timestamp casts following ANSI SQL
semantics, using `Presto as a reference implementation <https://prestodb.io/docs/current/functions/datetime.html>`_,
using ``set session legacy_timestamp = false;`` (see the section below for
details).

Timestamp literals are created based on whether time zone information is found
on the string on not:

::

  SELECT typeof(TIMESTAMP '1970-01-01 00:00:00'); -- timestamp
  SELECT typeof(TIMESTAMP '1970-01-01 00:00:00 UTC'); -- timestamp with time zone 

Converting a TimestampWithTimezone into a Timestamp works by dropping the
timezone information and returning only the timestamp portion:

::

  SELECT cast(TIMESTAMP '1970-01-01 00:00:00 UTC' as timestamp); -- 1970-01-01 00:00:00.000 
  SELECT cast(TIMESTAMP '1970-01-01 00:00:00 America/New_York' as timestamp); -- 1970-01-01 00:00:00.000 

To convert a Timestamp into a TimestampWithTimezone, one needs to specify a
timezone. In Presto, the session timezone is used by default:

::

  SELECT current_timezone(); -- America/Los_Angeles
  SELECT cast(TIMESTAMP '1970-01-01 00:00:00' as timestamp with time zone); -- 1970-01-01 00:00:00.000 America/Los_Angeles 

Conversion across TimestampWithTimezone can be done using the AT TIME ZONE
construct. 

The semantic of this operation is: at the absolute point in time described by
the source TimestampWithTimezone (``1970-01-01 00:00:00 UTC``), what would be
the clock/calendar read at the target timezone (Los Angeles)?

::

  SELECT TIMESTAMP '1970-01-01 00:00:00 UTC' AT TIME ZONE 'America/Los_Angeles'; -- 1969-12-31 16:00:00.000 America/Los_Angeles 
  SELECT TIMESTAMP '1970-01-01 00:00:00 UTC' AT TIME ZONE 'UTC'; -- 1970-01-01 00:00:00.000 UTC 

Strings can be converted into Timestamp and TimestampWithTimezone:

::

  SELECT cast('1970-01-01 00:00:00' as timestamp); -- 1970-01-01 00:00:00.000 
  SELECT cast('1970-01-01 00:00:00 America/Los_Angeles' as timestamp with time zone); -- 1970-01-01 00:00:00.000 America/Los_Angeles 

One can also convert a TimestampWithTimezone into a unix epoch/time. The
semantic of this operation is: at the absolute point in time described by the
timestamp with timezone taken as a parameter, what was the unix epoch? Remember
that unix epoch is the number of seconds since ``1970-01-01 00:00:00`` in UTC:

::

  SELECT to_unixtime(TIMESTAMP '1970-01-01 00:00:00 UTC'); -- 0.0 
  SELECT to_unixtime(TIMESTAMP '1970-01-01 00:00:00 America/Los_Angeles'); -- 28800.0 

The opposite conversion can be achieved using ``from_unixtime()``. The function
may take an optional second parameter to specify the timezone, having the same
semantic as AT TIME ZONE described above:

::

  SELECT from_unixtime(0); -- 1970-01-01 00:00:00.000 
  SELECT from_unixtime(0, 'UTC'); -- 1970-01-01 00:00:00.000 UTC 
  SELECT from_unixtime(0, 'America/Los_Angeles'); -- 1969-12-31 16:00:00.000 America/Los_Angeles

Presto Cast Legacy Behavior
---------------------------

For historical reasons, Presto used to interpret a TIMESTAMP as an absolute
point in time at the user’s time zone, instead of a timezone-less wall clock
reading as the ANSII SQL defines it. More information
`can be found here <https://github.com/prestodb/presto/issues/7122>`_. 

Although this has been fixed in newer versions, a ``legacy_timestamp`` session
flag was added  to preserve backwards compatibility. When this flag is set,
timestamps have a different semantic:

::

  SET SESSION legacy_timestamp = true;
  SELECT cast(TIMESTAMP '1970-01-01 00:00:00 UTC' as timestamp); -- 1969-12-31 16:00:00.000 
  SELECT cast('1970-01-01 00:00:00 UTC' as timestamp); -- 1969-12-31 16:00:00.000 

To support the two timestamp semantics, the
``core::QueryConfig::kAdjustTimestampToTimezone`` query flag was added to Velox.
When this flag is set, Velox will convert the timestamp into the user’s session
time zone to follow the expected semantic, although non-ANSI SQL compliant.

Other Resources
---------------

* https://github.com/prestodb/presto/issues/7122
* https://github.com/a0x8o/presto/blob/master/presto-docs/src/main/sphinx/language/timestamp.rst
* https://github.com/facebookincubator/velox/issues/8037
