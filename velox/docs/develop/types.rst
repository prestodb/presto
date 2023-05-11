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
DATE               struct Date                   8
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
DECIMAL                 BIGINT if precision <= 18, HUGEINT if precision >= 19
INTERVAL DAY TO SECOND  BIGINT
======================  ======================================================

We are in the process of migrating (:pr:`4744`) DATE type to a logical type backed
by BIGINT.

DECIMAL type carries additional `precision`,
and `scale` information. `Precision` is the number of
digits in a number. `Scale` is the number of digits to the right of the decimal
point in a number. For example, the number `123.45` has a precision of `5` and a
scale of `2`. DECIMAL types are backed by `BIGINT` and `HUGEINT` physical types,
which store the unscaled value. For example, the unscaled value of decimal
`123.45` is `12345`. `BIGINT` is used upto 18 precision, and has a range of
[:math:`-10^{18} + 1, +10^{18} - 1`]. `HUGEINT` is used starting from 19 precision
upto 38 precision, with a range of [:math:`-10^{38} + 1, +10^{38} - 1`].

All the three values, precision, scale, unscaled value are required to represent a
decimal value.

Custom Types
~~~~~~~~~~~~
Most custom types can be represented as logical types and can be built by extending
the existing physical types. For example, Presto Types described below are implemented
by extending the physical types.
An OPAQUE type must be used when there is no physical type available to back the logical type.

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
TIMESTAMP WITH TIME ZONE  ROW<BIGINT, SMALLINT>
========================  =====================

