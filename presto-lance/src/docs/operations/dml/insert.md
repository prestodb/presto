# INSERT INTO

Appends new rows to an existing Lance table.

## Syntax

```sql
INSERT INTO table_name [ ( column_name, ... ) ]
VALUES ( value, ... ), ...
-- or
INSERT INTO table_name
SELECT ...
```

## Examples

### Insert values directly
```sql
INSERT INTO lance.default.vector_embeddings (id, embedding, description)
VALUES 
(1, ARRAY[0.1, 0.2, 0.3], 'embedding one'),
(2, ARRAY[0.4, 0.5, 0.6], 'embedding two');
```

### Insert from select query (CTAS equivalent append)
```sql
INSERT INTO lance.default.vector_embeddings
SELECT id, embedding, description FROM other_catalog.other_schema.source_table;
```

## Limitations

* **No Update/Delete/Merge:** `UPDATE`, `DELETE`, and `MERGE` SQL statements are not supported by the connector.
* **Schema Validation:** The schemas of the values/rows being inserted must exactly match the schema defined in the table.
