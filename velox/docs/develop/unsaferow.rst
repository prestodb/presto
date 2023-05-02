==============================
UnsafeRow Serialization Format
==============================

Velox supports two data serialization formats out of the box:
`PrestoPage <https://prestodb.io/docs/current/develop/serialized-page.html>`_
and UnsafeRow. These formats are used in data shuffle. Velox applications
can register their own formats as well.

PrestoPage format is described in the `Presto documentation <https://prestodb.io/docs/current/develop/serialized-page.html>`_.
This article describes UnsafeRow format which comes from `Apache Spark <https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-UnsafeRow.html>`_.

A row is a continuous buffer made of 3 sections:

null bits | fixed-width data | variable-width data

Each section is aligned to 8 bytes.

The 'null bits' section contains one bit per column. 0 bit indicates a
non-null value. 1 bit indicates a null value.

The 'fixed-width data' section contains 8 bytes per column. Values of the
fixed-width columns (booleans, integers, floating point numbers) are stored
directly. These values must fit within 8 bytes. Long decimal columns are not
supported.

Values of the variable-width columns (strings, arrays, maps) are split between
fixed-width and variable-width sections. 8 bytes of the fixed-width section
store the size and location of the value in the variable-width section.

The 'variable-width data' stores values of the variable-width columns. Each
value is aligned to 8 bytes.

Strings are stored directly without null-terminating byte.

Arrays are stored as 8 bytes for the size of the array followed by array
elements serialized mostly as UnsafeRow. Fixed-width array elements are
serialized using only necessary number of bytes, i.e. TINYINT and BOOLEAN
elements use 1 byte per element, SMALLINT elements use 2 bytes per element,
BIGINT elements use 8 bytes per element. The 'fixed-width data' section
is still aligned at 8 bytes though.

array size | null bits | fixed-width data | variable-width data

The 'null bits' stores one bit per array element and indicates which
elements are null.

Maps are stored as 8 bytes for the size of serialized keys array followed
by arrays of keys and array of values serialized as UnsafeRow.

size of serialized keys array in bytes | <keys array> | <values array>

, where <keys array> is

number of keys | null bits | fixed-width data | variable-width data

and <values array> is

number of values | null bits | fixed-width data | variable-width data

Number of keys and number of values above are the same and equal to the
map size. These are duplicated in the serialized data.

Structs are stored as 'null bits' for struct fields, followed by
fixed-width field values and variable-width field values.

Examples
--------

A row with two columns, INTEGER and BIGINT, has fixed serialized size of 24
bytes. 8 bytes for null flags. 8 bytes for the value of the first column.
8 bytes for the value of the second column. Note that we use at least 8 bytes
for any value, including BOOLEAN and TINYINT.

A row with a single ARRAY of BIGINT has variable serialized size. An array
of 10 elements [0, 11, 22, 33, 44, 55, 66, 77, 88, 99] uses 112 bytes.

* 8 bytes for null flags.
* 8 bytes for size and offset of variable-width data.
* 8 bytes for array size (10).
* 8 bytes for null flags for array elements.
* 80 (= 8 * 10) bytes for 10 fixed-width array elements.

A row with a single ARRAY of TINYINT has variable serialized size. An array
of 10 elements [0, 11, 22, 33, 44, 55, 66, 77, 88, 99] uses 48 bytes.

* 8 bytes for null flags.
* 8 bytes for size and offset of variable-width data.
* 8 bytes for array size (10).
* 8 bytes for null flags for array elements.
* 16 bytes for 10 fixed-width array elements (1 byte per element aligned at 8 bytes).

A row with a single MAP of BIGINT to BIGINT has variable serialized size. A map
of size 3 [1 => 10, 2 => 20, 3 => 30] uses 104 bytes.

* 8 bytes for null flags.
* 8 bytes for size and offset of variable-width data.
* 8 bytes for the size of serialized keys array (40).
* 8 bytes for the number of keys.
* 8 bytes for null flags for keys.
* 24 (= 8 * 3) bytes for 3 fixed-width keys.
* 8 bytes for the number of values.
* 8 bytes for null flags for values.
* 24 (= 8 * 3) bytes for 3 fixed-width values.

A row with a singe struct of BIGINT and DOUBLE has fixed serialized size of 40 bytes.

* 8 bytes for null flags.
* 8 bytes for size and offset of variable-width data.
* 8 bytes for null flags of the struct fields.
* 8 bytes for the value of the first struct field.
* 8 bytes for the value of the second struct field.

Batches of Rows
---------------

It is common for engines to require the serialization of a batch of rows. In
these cases, a batch of serialized UnsafeRows can be created by successively
serializing the row size then the UnsafeRow buffer, in the following manner:

row size | UnsafeRow | row size | UnsafeRow | ...

Be careful that the `row size` integer needs to be of **4 bytes**, and encoded using
**big endian** format. Note that this is different from the other integers serialized as
part of the UnsafeRow payload, which are all expected to be little endian.
