# DROP TABLE

Deletes a Lance table from the filesystem.

## Syntax

```sql
DROP TABLE [ IF EXISTS ] table_name
```

## Example

```sql
DROP TABLE lance.default.vector_embeddings;
```

## Remarks

* Dropping a table will completely remove the table subdirectory (e.g. `vector_embeddings.lance`) and all its underlying fragments and metadata files from the configured `lance.root-url`.
* This operation is irreversible.
