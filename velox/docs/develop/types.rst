=====
Types
=====

Velox supports scalar types and complex types. These types cover most of the
Presto and Spark data types.

Scalar Types
~~~~~~~~~~~~
Scalar types in Velox are logical and SQL-compatible.
Each scalar type is implemented using a C++ type. The table
below shows the supported scalar types and their corresponding C++ type.

======================  ===========================    ==================
Velox Type              C++ Type                       Bytes per Value
======================  ===========================    ==================
BOOLEAN                 bool                            0.125 (i.e. 1 bit)
TINYINT                 int8_t                          1
SMALLINT                int16_t                         2
INTEGER                 int32_t	                        4
BIGINT                  int64_t                         8
DATE                    struct Date                     8
REAL                    float                           4
DOUBLE                  double                          8
SHORT_DECIMAL           struct UnscaledShortDecimal     8
LONG_DECIMAL            struct UnscaledLongDecimal     16
TIMESTAMP               struct Timestamp               16
INTERVAL DAY TO SECOND  struct IntervalDayTime          8
VARCHAR                 struct StringView              16
VARBINARY               struct StringView              16
======================  ===========================    ==================

All scalar types except DECIMAL have a one-to-one mapping with their C++ types.

DECIMAL type is a special scalar type since it carries additional `precision`,
and `scale` information. Similar to SQL decimal, `precision` is the number of
digits in a number. `Scale` is the number of digits to the right of the decimal
point in a number. For example, the number `123.45` has a precision of `5` and a
scale of `2`. Velox supports two types of decimals, SHORT_DECIMAL and LONG_DECIMAL.
SHORT_DECIMAL has a maximum precision of 18, with a range of
[:math:`-10^{18} + 1, +10^{18} - 1`]. LONG_DECIMAL has a maximum precision of 38,
with a range of [:math:`-10^{38} + 1, +10^{38} - 1`].
Their corresponding C++ types, UnscaledShortDecimal and UnscaledLongDecimal carry
the unscaled value. For example, the unscaled value of decimal `123.45` is `12345`.
All the three values, precision, scale, unscaled value are required to represent a
decimal value. UnscaledLongDecimal is a wrapper around the `int128_t` type.
Some systems implement a similar wrapper around two `int64_t` values.
Velox chose `int128_t` since most compilers now support this type and
it simplifies the implementation.


Complex Types
~~~~~~~~~~~~~
Velox supports the ARRAY, MAP, and ROW complex types.

