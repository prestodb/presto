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

.. spark:function:: dayofweek(date/timestamp) -> integer

    Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
    We can use `dow` as alias for ::

        SELECT dayofweek('2009-07-30'); -- 5
        SELECT dayofweek('2023-08-22 11:23:00.100'); -- 3

.. spark::function:: dow(x) -> integer

    This is an alias for :func:`day_of_week`.

.. spark::function::from_unixtime(unixTime, format) -> string

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

.. function:: get_timestamp(string, dateFormat) -> timestamp

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

.. spark:function:: quarter(date) -> integer

    Returns the quarter of ``date``. The value ranges from ``1`` to ``4``. ::

        SELECT quarter('2009-07-30'); -- 3

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

.. spark:function:: to_unix_timestamp(string) -> integer

    Alias for ``unix_timestamp(string) -> integer``.

.. spark:function:: to_unix_timestamp(string, format) -> integer
   :noindex:

    Alias for ``unix_timestamp(string, format) -> integer``.

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

.. _ISO-Week: https://en.wikipedia.org/wiki/ISO_week_date

.. spark:function:: year(x) -> integer

    Returns the year from ``x``.
