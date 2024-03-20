=====================================
Date and Time Functions
=====================================

Convenience Extraction Functions
--------------------------------

These functions support TIMESTAMP and DATE input types.

.. spark:function:: add_months(startDate, numMonths) -> date

    Returns the date that is ``numMonths`` after ``startDate``.
    Adjusts result to a valid one, considering months have different total days, and especially
    February has 28 days in common year but 29 days in leap year.
    For example, add_months('2015-01-30', 1) returns '2015-02-28', because 28th is the last day
    in February of 2015.
    ``numMonths`` can be zero or negative. Throws an error when inputs lead to int overflow,
    e.g., add_months('2023-07-10', -2147483648). ::

        SELECT add_months('2015-01-01', 10); -- '2015-11-01'
        SELECT add_months('2015-01-30', 1); -- '2015-02-28'
        SELECT add_months('2015-01-30', 0); -- '2015-01-30'
        SELECT add_months('2015-01-30', -2); -- '2014-11-30'
        SELECT add_months('2015-03-31', -1); -- '2015-02-28'

.. spark:function:: date_add(start_date, num_days) -> date

    Returns the date that is ``num_days`` after ``start_date``. According to the inputs,
    the returned date will wrap around between the minimum negative date and
    maximum positive date. date_add('1969-12-31', 2147483647) get 5881580-07-10,
    and date_add('2024-01-22', 2147483647) get -5877587-07-12.

    If ``num_days`` is a negative value then these amount of days will be
    deducted from ``start_date``.
    Supported types for ``num_days`` are: TINYINT, SMALLINT, INTEGER.

.. spark:function:: date_from_unix_date(integer) -> date

    Creates date from the number of days since 1970-01-01 in either direction. Returns null when input is null.

        SELECT date_from_unix_date(1); -- '1970-01-02'
        SELECT date_from_unix_date(-1); -- '1969-12-31'

.. spark:function:: date_sub(start_date, num_days) -> date

    Returns the date that is ``num_days`` before ``start_date``. According to the inputs,
    the returned date will wrap around between the minimum negative date and
    maximum positive date. date_sub('1969-12-31', -2147483648) get 5881580-07-11,
    and date_sub('2023-07-10', -2147483648) get -5877588-12-29.

    ``num_days`` can be positive or negative.
    Supported types for ``num_days`` are: TINYINT, SMALLINT, INTEGER.

.. spark:function:: datediff(endDate, startDate) -> integer

    Returns the number of days from startDate to endDate. Only DATE type is allowed
    for input. ::

        SELECT datediff('2009-07-31', '2009-07-30'); -- 1
        SELECT datediff('2009-07-30', '2009-07-31'); -- -1

.. spark:function:: dayofmonth(date) -> integer

    Returns the day of month of the date. ::

        SELECT dayofmonth('2009-07-30'); -- 30

.. spark:function:: dayofyear(date) -> integer

    Returns the day of year of the date. ::

        SELECT dayofyear('2016-04-09'); -- 100

.. spark:function:: dayofweek(date) -> integer

    Returns the day of the week for date (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

        SELECT dayofweek('2009-07-30'); -- 5
        SELECT dayofweek('2023-08-22'); -- 3

.. spark:function:: from_unixtime(unixTime, format) -> string

    Adjusts ``unixTime`` (elapsed seconds since UNIX epoch) to configured session timezone, then
    converts it to a formatted time string according to ``format``. Only supports BIGINT type for
    ``unixTime``.
    `Valid patterns for date format
    <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_. Throws exception for
    invalid ``format``. This function will convert input to milliseconds, and integer overflow is
    allowed in the conversion, which aligns with Spark. See the below third example where INT64_MAX
    is used, -1000 milliseconds are produced by INT64_MAX * 1000 due to integer overflow. ::

        SELECT from_unixtime(100, 'yyyy-MM-dd HH:mm:ss'); -- '1970-01-01 00:01:40'
        SELECT from_unixtime(3600, 'yyyy'); -- '1970'
        SELECT from_unixtime(9223372036854775807, "yyyy-MM-dd HH:mm:ss");  -- '1969-12-31 23:59:59'

.. spark:function:: from_utc_timestamp(timestamp, string) -> timestamp

    Returns the timestamp value from UTC timezone to the given timezone. ::

        SELECT from_utc_timestamp('2015-07-24 07:00:00', 'America/Los_Angeles'); -- '2015-07-24 00:00:00'

.. spark:function:: get_timestamp(string, dateFormat) -> timestamp

    Returns timestamp by parsing ``string`` according to the specified ``dateFormat``.
    The format follows Spark's
    `Datetime patterns
    <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.
    Returns NULL for parsing error or NULL input. Throws exception for invalid format. ::

        SELECT get_timestamp('1970-01-01', 'yyyy-MM-dd);  -- timestamp `1970-01-01`
        SELECT get_timestamp('1970-01-01', 'yyyy-MM');  -- NULL (parsing error)
        SELECT get_timestamp('1970-01-01', null);  -- NULL
        SELECT get_timestamp('2020-06-10', 'A');  -- (throws exception)

.. spark:function:: hour(timestamp) -> integer

    Returns the hour of ``timestamp``.::

        SELECT hour('2009-07-30 12:58:59'); -- 12

.. spark:function:: last_day(date) -> date

    Returns the last day of the month which the date belongs to.

.. spark:function:: make_date(year, month, day) -> date

    Returns the date from year, month and day fields.
    ``year``, ``month`` and ``day`` must be ``INTEGER``.
    Throws an error if inputs are not valid.

    The valid inputs need to meet the following conditions,
    ``month`` need to be from 1 (January) to 12 (December).
    ``day`` need to be from 1 to 31, and matches the number of days in each month.
    days of ``year-month-day - 1970-01-01`` need to be in the range of INTEGER type.

.. spark:function:: make_ym_interval([years[, months]]) -> interval year to month

    Make year-month interval from ``years`` and ``months`` fields.
    Returns the actual year-month with month in the range of [0, 11].
    Both ``years`` and ``months`` can be zero, positive or negative.
    Throws an error when inputs lead to int overflow,
    e.g., make_ym_interval(178956970, 8). ::

        SELECT make_ym_interval(1, 2); -- 1-2
        SELECT make_ym_interval(1, 0); -- 1-0
        SELECT make_ym_interval(-1, 1); -- -0-11
        SELECT make_ym_interval(1, 100); -- 9-4
        SELECT make_ym_interval(1, 12); -- 2-0
        SELECT make_ym_interval(1, -12); -- 0-0
        SELECT make_ym_interval(2); -- 2-0
        SELECT make_ym_interval(); -- 0-0

.. spark:function:: minute(timestamp) -> integer

    Returns the minutes of ``timestamp``.::

        SELECT minute('2009-07-30 12:58:59'); -- 58

.. spark:function:: quarter(date) -> integer

    Returns the quarter of ``date``. The value ranges from ``1`` to ``4``. ::

        SELECT quarter('2009-07-30'); -- 3

.. spark:function:: make_timestamp(year, month, day, hour, minute, second[, timezone]) -> timestamp

    Create timestamp from ``year``, ``month``, ``day``, ``hour``, ``minute`` and ``second`` fields.
    If the ``timezone`` parameter is provided,
    the function interprets the input time components as being in the specified ``timezone``.
    Otherwise the function assumes the inputs are in the session's configured time zone.
    Requires ``session_timezone`` to be set, or an exceptions will be thrown.

    Arguments:
        * year - the year to represent, within the Joda datetime
        * month - the month-of-year to represent, from 1 (January) to 12 (December)
        * day - the day-of-month to represent, from 1 to 31
        * hour - the hour-of-day to represent, from 0 to 23
        * minute - the minute-of-hour to represent, from 0 to 59
        * second - the second-of-minute and its micro-fraction to represent, from 0 to 60.
          The value can be either an integer like 13, or a fraction like 13.123.
          The fractional part can have up to 6 digits to represent microseconds.
          If the sec argument equals to 60, the seconds field is set
          to 0 and 1 minute is added to the final timestamp.
        * timezone - the time zone identifier. For example, CET, UTC and etc.

    Returns the timestamp adjusted to the GMT time zone.
    Returns NULL for invalid or NULL input. ::

        SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887); -- 2014-12-28 06:30:45.887
        SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887, 'CET'); -- 2014-12-28 05:30:45.887
        SELECT make_timestamp(2019, 6, 30, 23, 59, 60); -- 2019-07-01 00:00:00
        SELECT make_timestamp(2019, 6, 30, 23, 59, 1); -- 2019-06-30 23:59:01
        SELECT make_timestamp(null, 7, 22, 15, 30, 0); -- NULL
        SELECT make_timestamp(2014, 12, 28, 6, 30, 60.000001); -- NULL
        SELECT make_timestamp(2014, 13, 28, 6, 30, 45.887); -- NULL

.. spark:function:: month(date) -> integer

    Returns the month of ``date``. ::

        SELECT month('2009-07-30'); -- 7

.. spark:function:: next_day(startDate, dayOfWeek) -> date

    Returns the first date which is later than ``startDate`` and named as ``dayOfWeek``.
    Returns null if ``dayOfWeek`` is invalid.
    ``dayOfWeek`` is case insensitive and must be one of the following:
    ``SU``, ``SUN``, ``SUNDAY``, ``MO``, ``MON``, ``MONDAY``, ``TU``, ``TUE``, ``TUESDAY``,
    ``WE``, ``WED``, ``WEDNESDAY``, ``TH``, ``THU``, ``THURSDAY``, ``FR``, ``FRI``, ``FRIDAY``,
    ``SA``, ``SAT``, ``SATURDAY``. ::

        SELECT next_day('2015-07-23', "Mon"); -- '2015-07-27'
        SELECT next_day('2015-07-23', "mo"); -- '2015-07-27'
        SELECT next_day('2015-07-23', "Tue"); -- '2015-07-28'
        SELECT next_day('2015-07-23', "tu"); -- '2015-07-28'
        SELECT next_day('2015-07-23', "we"); -- '2015-07-29'

.. spark:function:: second(timestamp) -> integer

    Returns the seconds of ``timestamp``.::

        SELECT second('2009-07-30 12:58:59'); -- 59

.. spark:function:: to_unix_timestamp(string) -> integer

    Alias for ``unix_timestamp(string) -> integer``.

.. spark:function:: to_unix_timestamp(string, format) -> integer
   :noindex:

    Alias for ``unix_timestamp(string, format) -> integer``.

.. spark:function:: to_utc_timestamp(timestamp, string) -> timestamp

    Returns the timestamp value from the given timezone to UTC timezone. ::

        SELECT to_utc_timestamp('2015-07-24 00:00:00', 'America/Los_Angeles'); -- '2015-07-24 07:00:00'

.. spark:function:: unix_date(date) -> integer

    Returns the number of days since 1970-01-01.::

        SELECT unix_date('1970-01-01'); -- '0'
        SELECT unix_date('1970-01-02'); -- '1'
        SELECT unix_date('1969-12-31'); -- '-1'

.. spark:function:: unix_timestamp() -> integer

    Returns the current UNIX timestamp in seconds.

.. spark:function:: unix_timestamp(string) -> integer
   :noindex:

    Returns the UNIX timestamp of time specified by ``string``. Assumes the 
    format ``yyyy-MM-dd HH:mm:ss``. Returns null if ``string`` does not match
    ``format``.

.. spark:function:: unix_timestamp(string, format) -> integer
   :noindex:

    Returns the UNIX timestamp of time specified by ``string`` using the
    format described in the ``format`` string. The format follows Spark's
    `Datetime patterns for formatting and parsing
    <https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.
    Returns null if ``string`` does not match ``format`` or if ``format``
    is invalid.

.. function:: week_of_year(x) -> integer

    Returns the `ISO-Week`_ of the year from x. The value ranges from ``1`` to ``53``.
    A week is considered to start on a Monday and week 1 is the first week with >3 days.

.. function:: weekday(date) -> integer

    Returns the day of the week for date (0 = Monday, 1 = Tuesday, …, 6 = Sunday).::

        SELECT weekday('2015-04-08'); -- 2
        SELECT weekday('2024-02-10'); -- 5

.. _ISO-Week: https://en.wikipedia.org/wiki/ISO_week_date

.. spark:function:: year(x) -> integer

    Returns the year from ``x``.
