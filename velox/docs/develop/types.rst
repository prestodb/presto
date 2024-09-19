=====
Types
=====

Velox supports scalar types and complex types.
Scalar types are categorized into a fixed set of physical types,
and an extensible set of logical types.
Physical types determine the in-memory layout of the data.
Logical types add additional semantics to a physical type.

Physical Types
~~~~~~~~~~~~~~
Each physical type is implemented using a C++ type. The table
below shows the supported physical types, their corresponding C++ type,
and fixed-width bytes required per value.

================   ===========================   ===================
Physical Type      C++ Type                      Fixed Width (bytes)
================   ===========================   ===================
BOOLEAN            bool                          0.125 (i.e. 1 bit)
TINYINT            int8_t                        1
SMALLINT           int16_t                       2
INTEGER            int32_t                       4
BIGINT             int64_t                       8
HUGEINT            int128_t                      16
REAL               float                         4
DOUBLE             double                        8
TIMESTAMP          struct Timestamp              16
VARCHAR            struct StringView             16
VARBINARY          struct StringView             16
OPAQUE             std::shared_ptr<void>         16
UNKNOWN            struct UnknownValue           0
================   ===========================   ===================

All physical types except VARCHAR and VARBINARY have a one-to-one mapping
with their C++ types.
The C++ type is also used as a template parameter for vector classes.
For example, vector of 64-bit integers is represented as `FlatVector<int64_t>`
whose type is `BIGINT`.

OPAQUE type can be used to define custom types.
An OPAQUE type must be specified wih an unique `std::type_index`.
Values for this type must be provided as `std::shared_ptr<T>` where T is a C++ type.
More details on when to use an OPAQUE type to define a custom type are given below.

VARCHAR, VARBINARY, OPAQUE use variable number of bytes per value.
These types store a fixed-width part in the C++ type and a variable-width part elsewhere.
All other types use fixed-width bytes per value as shown in the above table.
For example: VARCHAR and VARBINARY :doc:`FlatVectors </develop/vectors>` store the
fixed-width part in a StringView for each value.
StringView is a struct that contains a 4-byte size field, a 4-byte prefix field,
and an 8-byte field pointer that points to variable-width part.
The variable-width part for each value is store in `stringBuffers`.
OPAQUE types store variable-width parts outside of the FlatVector.

UNKNOWN type is used to represent an empty or all nulls vector of unknown type.
For example, SELECT array() returns an ARRAY(UNKNOWN()) because it is not possible
to determine the type of the elements. This works because there are no elements.

TIMESTAMP type is used to represent a specific point in time.
A TIMESTAMP is defined as the sum of seconds and nanoseconds since UNIX epoch.
`struct Timestamp` contains one 64-bit signed integer for seconds and another 64-bit
unsigned integer for nanoseconds. Nanoseconds represent the high-precision part of
the timestamp, which is less than 1 second. Valid range of nanoseconds is [0, 10^9).
Timestamps before the epoch are specified using negative values for the seconds.
Examples:

* Timestamp(0, 0) represents 1970-01-0 T00:00:00 (epoch).
* Timestamp(10*24*60*60 + 125, 0) represents 1970-01-11 00:02:05 (10 days 125 seconds after epoch).
* Timestamp(19524*24*60*60 + 500, 38726411) represents 2023-06-16 08:08:20.038726411
  (19524 days 500 seconds 38726411 nanoseconds after epoch).
* Timestamp(-10*24*60*60 - 125, 0) represents 1969-12-21 23:57:55 (10 days 125 seconds before epoch).
* Timestamp(-5000*24*60*60 - 1000, 123456) represents 1956-04-24 07:43:20.000123456
  (5000 days 1000 seconds before epoch plus 123456 nanoseconds).

Floating point types (REAL, DOUBLE) have special values negative infinity, positive infinity, and
not-a-number (NaN).

For NaN the semantics are different than the C++ standard floating point semantics:

* The different types of NaN (+/-, signaling/quiet) are treated as canonical NaN (+, quiet).
* `NaN = NaN` returns true.
* NaN is treated as a normal numerical value in join and group-by keys.
* When sorting, NaN values are considered larger than any other value. When sorting in ascending order, NaN values appear last. When sorting in descending order, NaN values appear first.
* For a number N: `N > NaN` is false and `NaN > N` is true.

For negative infinity and positive infinity the following C++ standard floating point semantics apply:

Given N is a positive finite number.

* +inf * N = +inf
* -inf * N = -inf
* +inf * -N = -inf
* -inf * -N = +inf
* +inf * 0 = NaN
* -inf * 0 = NaN
* +inf = +inf returns true.
* -inf = -inf returns true.
* Positive infinity and negative infinity are treated as normal numerical values in join and group-by keys.
* Positive infinity sorts lower than NaN and higher than any other value.
* Negative infinity sorts lower than any other value.

Logical Types
~~~~~~~~~~~~~
Logical types are backed by a physical type and include additional semantics.
There can be multiple logical types backed by the same physical type.
Therefore, knowing the C++ type is not sufficient to infer a logical type.
The table below shows the supported logical types, and
their corresponding physical type.

======================  ======================================================
Logical Type            Physical Type
======================  ======================================================
DATE                    INTEGER
DECIMAL                 BIGINT if precision <= 18, HUGEINT if precision >= 19
INTERVAL DAY TO SECOND  BIGINT
INTERVAL YEAR TO MONTH  INTEGER
======================  ======================================================

DECIMAL type carries additional `precision`,
and `scale` information. `Precision` is the number of
digits in a number. `Scale` is the number of digits to the right of the decimal
point in a number. For example, the number `123.45` has a precision of `5` and a
scale of `2`. DECIMAL types are backed by `BIGINT` and `HUGEINT` physical types,
which store the unscaled value. For example, the unscaled value of decimal
`123.45` is `12345`. `BIGINT` is used upto 18 precision, and has a range of
:math:`[-10^{18} + 1, +10^{18} - 1]`. `HUGEINT` is used starting from 19 precision
upto 38 precision, with a range of :math:`[-10^{38} + 1, +10^{38} - 1]`.

All the three values, precision, scale, unscaled value are required to represent a
decimal value.

Custom Types
~~~~~~~~~~~~
Most custom types can be represented as logical types and can be built by extending
the existing physical types. For example, Presto Types described below are implemented
by extending the physical types.
An OPAQUE type must be used when there is no physical type available to back the logical type.

When extending an existing physical type, if different compare and/or hash semantics are
needed instead of those provided by the underlying native C++ type, this can be achieved by
doing the following:
* Pass `true` for the `providesCustomComparison` argument in the custom type's base class's constructor.
* Override the `compare` and `hash` functions inherited from the `TypeBase` class (you must implement both).
Note that this is currently only supported for custom types that extend physical types that
are primitive and fixed width.

Complex Types
~~~~~~~~~~~~~
Velox supports the ARRAY, MAP, and ROW complex types.
Complex types are composed of scalar types and can be nested with
other complex types.

For example: MAP<INTEGER, ARRAY<BIGINT>> is a complex type whose
key is a scalar type INTEGER and value is a complex type ARRAY with
element type BIGINT.

Array type contains its element type.
Map type contains the key type and value type.
Row type contains its field types along with their names.

Presto Types
~~~~~~~~~~~~
Velox supports a number of Presto-specific logical types.
The table below shows the supported Presto types.

========================  =====================
Presto Type               Physical Type
========================  =====================
HYPERLOGLOG               VARBINARY
JSON                      VARCHAR
TIMESTAMP WITH TIME ZONE  BIGINT
UUID                      HUGEINT
IPADDRESS                 HUGEINT
========================  =====================

TIMESTAMP WITH TIME ZONE represents a time point in milliseconds precision
from UNIX epoch with timezone information. Its physical type is BIGINT.
The high 52 bits of bigint store signed integer for milliseconds in UTC.
Supported range of milliseconds is [0xFFF8000000000000L, 0x7FFFFFFFFFFFF]
(or [-69387-04-22T03:45:14.752, 73326-09-11T20:14:45.247]). The low 12 bits
store timezone ID. Supported range of timezone ID is [1, 1680].
The definition of timezone IDs can be found in ``TimeZoneDatabase.cpp``.

IPADDRESS represents an IPV6 or IPV4 formatted IPV6 address. Its physical
type is HUGEINT. The format that the address is stored in is defined as part of `(RFC 4291#section-2.5.5.2) <https://datatracker.ietf.org/doc/html/rfc4291.html#section-2.5.5.2>`_
As Velox is run on Little Endian systems and the standard is network byte(Big Endian)
order, we reverse the bytes to allow for masking and other bit operations
used in IPADDRESS/IPPREFIX related functions. This type can be used to
create IPPREFIX networks as well as to check IPADDRESS validity within
IPPREFIX networks.

Spark Types
~~~~~~~~~~~~
The `data types <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>`_ in Spark have some semantic differences compared to those in
Presto. These differences require us to implement the same functions
separately for each system in Velox, such as min, max and collect_set. The
key differences are listed below.

* Spark operates on timestamps with "microsecond" precision while Presto with
  "millisecond" precision.
  Example::

      SELECT min(ts)
      FROM (
          VALUES
              (cast('2014-03-08 09:00:00.123456789' as timestamp)),
              (cast('2014-03-08 09:00:00.012345678' as timestamp))
      ) AS t(ts);
      -- 2014-03-08 09:00:00.012345

* In function comparisons, nested null values are handled as values.
  Example::

      SELECT equalto(ARRAY[1, null], ARRAY[1, null]); -- true

      SELECT min(a)
      FROM (
          VALUES
              (ARRAY[1, 2]),
              (ARRAY[1, null])
      ) AS t(a);
      -- ARRAY[1, null]

* MAP type is not comparable and not orderable in Spark. In Presto, MAP type is
  also not orderable, but it is comparable if both key and value types are
  comparable. The implication is that MAP type cannot be used as a join, group
  by or order by key in Spark.
