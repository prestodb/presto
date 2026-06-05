# CREATE TABLE

Creates a new empty Lance table with the specified schema.

## Syntax

```sql
CREATE TABLE [ IF NOT EXISTS ] table_name (
    column_name data_type,
    ...
)
```

## Example

```sql
CREATE TABLE lance.default.vector_embeddings (
    id bigint,
    embedding array(real),
    description varchar
);
```

## Remarks

* Since Lance operates on a flat namespace structure, tables are always created under the virtual `default` schema.
* Creating tables with partition schemes (e.g. `WITH (partitioned_by = ...)`) is not supported by the Lance connector.
* Table properties are not supported.
