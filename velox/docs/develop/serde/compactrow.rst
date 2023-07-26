==========
CompactRow
==========

CompactRow is a row-wise serialization format provided by Velox as an
alternative to UnsafeRow format. CompactRow is more space efficient then
UnsafeRow and results in fewer bytes shuffled which has a cascading effect on
CPU usage (for compression and checksumming) and memory (for buffering).

A row is a contiguous buffer that starts with null flags, followed by individual
fields.

nulls | field1 | field 2 | …

Nulls section uses one bit per field to indicate which fields are null. If there
are 10 fields, there will be 2 bytes of null flags (16 bits total, 10 bits
used, 6 bits unused).

Fixed-width fields (integers, boolean, floating point numbers) take up a fixed
number of bytes regardless of whether they are null or not. A row with 10
bigint fields takes up 2 + 10 * 8 = 82 bytes. 2 bytes for null flags + 8 bytes
per field.

The sizes of fixed-width fields are:

================   ==============================================
Type               Number of bytes used for serialization
================   ==============================================
BOOLEAN            1
TINYINT            1
SMALLINT           2
INTEGER            4
BIGINT             8
HUGEINT            16
REAL               4
DOUBLE             8
TIMESTAMP          8
UNKNOWN            0
================   ==============================================

Strings (VARCHAR and VARBINARY) use 4 bytes for size plus the length of the
string. Empty string uses 4 bytes. 1-character string uses 5 bytes.
20-character ASCII string uses 24 bytes. Null strings do not take up space
(other than one bit in the nulls section).

Arrays of fixed-width values or strings, e.g. arrays of integers, use 4 bytes
for the size of the array, a few bytes for nulls flags indicating null-ness of
the elements (1 bit per element) plus the space taken by the elements
themselves.

For example, an array of 5 integers [1, 2, 3, 4, 5] uses 4 bytes for size, 1
byte for 5 null flags and 5 * 4 bytes for 5 values. A total of 25 bytes.


============    ====    ========    ======  ======  ======  ======  ======
Description     Size    Nulls       Elem 1  Elem 2  Elem 3  Elem 4  Elem 5
============    ====    ========    ======  ======  ======  ======  ======
# of bytes      4       1           4       4       4       4       4
Value           5       00000000    1       2       3       4       5
============    ====    ========    ======  ======  ======  ======  ======

An array of 4 strings [null, “Abc”, null, “Mountains and rivers”] uses 36 bytes:

============    ====    ========    =======     ======  =======     =====================
Description     Size    Nulls       Size s2     s2      Size s4     s4
============    ====    ========    =======     ======  =======     =====================
# of bytes      4       1           4           3       4           20
Value           4       10100000    1           Abc     20          Mountains and rivers
============    ====    ========    =======     ======  =======     =====================

Serialization of an array of complex type elements, e.g. an array of arrays, maps or structs, includes a few additional fields: the total serialized size plus offset of each element in the serialized buffer.

- 4 bytes - array size.
- N bytes - null flags, 1 bit per element.
- 4 bytes - Total serialized size of the array excluding first 2 fields (size and nulls).
- 4 bytes per element - Offsets of the elements in the serialized buffer relative to the position right after the total serialized size.
- Elements.

For example, an array of integers [[1, 2, 3], [4, 5], [6]] uses N bytes:

- 4 bytes - size - 3
- 1 byte - nulls - 00000000
- 4 bytes - total serialized size - 55
- 4 bytes - offset of the 1st element - 12
- 4 bytes - offset of the 2nd element - 29
- 4 bytes - offset of the 3rd element - 42
- —-- Start of the 1st element: [1, 2, 3]
- 4 bytes - size - 3
- 1 byte - nulls - 00000000
- 4 bytes - element 1 - 1
- 4 bytes - element 2 - 2
- 4 bytes - element 3 - 3
- —-- Start of the 2nd element: [4, 5]
- 4 bytes - size - 2
- 1 byte - nulls - 00000000
- 4 bytes - element 1 - 4
- 4 bytes - element 2 - 5
- —-- Start of the 2nd element: [6]
- 4 bytes - size - 1
- 1 byte - nulls - 00000000
- 4 bytes - element 1 - 6

A map is serialized as the keys array followed by the values array.

A struct is serialized the same as the top-level row.

Compared to UnsafeRow, on average CompactRow serialization is about twice shorter. Some examples are:

======================  =========   ==========
Type                    UnsafeRow   CompactRow
======================  =========   ==========
INTEGER                 8           4
BIGINT                  8           8
REAL                    8           4
DOUBLE                  8           8
VARCHAR: “” (empty)     8           4
VARCHAR: “Abc”          16          7
======================  =========   ==========
