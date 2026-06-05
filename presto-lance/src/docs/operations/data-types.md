# Supported Data Types

The Presto LanceDB connector supports standard primitive and complex types by mapping Presto data types to Apache Arrow types.

| Presto Type | Arrow / Lance Type | Description |
|---|---|---|
| `BOOLEAN` | `Bool` | Boolean true/false |
| `TINYINT` | `Int (8-bit, signed)` | 8-bit signed integer |
| `SMALLINT` | `Int (16-bit, signed)` | 16-bit signed integer |
| `INTEGER` | `Int (32-bit, signed)` | 32-bit signed integer |
| `BIGINT` | `Int (64-bit, signed)` | 64-bit signed integer |
| `REAL` | `FloatingPoint (32-bit, single precision)` | 32-bit single precision float |
| `DOUBLE` | `FloatingPoint (64-bit, double precision)` | 64-bit double precision float |
| `VARCHAR` | `Utf8` / `LargeUtf8` | UTF-8 encoded string |
| `VARBINARY` | `Binary` / `LargeBinary` | Unstructured binary data |
| `DATE` | `Date (unit DAY)` | Calendar date |
| `TIMESTAMP` | `Timestamp (unit MICROSECOND)` | Timestamp without time zone |
| `ARRAY` | `List` / `FixedSizeList` | Ordered list of elements |
| `ROW` | `Struct` | Structured nested fields |

> [!NOTE]
> Fixed-size arrays (such as `FixedSizeList`) are read as Presto `ARRAY` types, which is useful for processing embeddings and high-dimensional vector representations.
