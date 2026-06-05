# DESCRIBE TABLE

Shows the column names, data types, and nullability properties of the specified Lance table.

## Syntax

```sql
DESCRIBE table_name
-- or
DESC table_name
```

## Example

```sql
DESCRIBE lance.default.vector_embeddings;
```

**Output:**

| Column | Type | Extra | Comment |
|---|---|---|---|
| `id` | `bigint` | | |
| `embedding` | `array(real)` | | |
| `description` | `varchar` | | |
