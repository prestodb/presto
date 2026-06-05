# SHOW COLUMNS

Lists the columns in a Lance table along with their data types and properties.

## Syntax

```sql
SHOW COLUMNS FROM table_name
```

## Example

```sql
SHOW COLUMNS FROM lance.default.vector_embeddings;
```

**Output:**

| Column | Type | Null | Key | Default | Extra |
|---|---|---|---|---|---|
| `id` | `bigint` | `true` | | | |
| `embedding` | `array(real)` | `true` | | | |
| `description` | `varchar` | `true` | | | |
