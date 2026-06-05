# SELECT

Queries data from a Lance table.

## Syntax

```sql
SELECT column_name, ...
FROM table_name
[ WHERE condition ]
[ LIMIT count ]
```

## Example

```sql
SELECT id, description 
FROM lance.default.vector_embeddings
WHERE id > 100 AND description LIKE 'embed%'
LIMIT 10;
```

---

## Filter Pushdown Optimization

The Presto LanceDB connector supports **Predicate Filter Pushdown**. This optimization translates Presto query engine filters directly into Lance's scanner filters. This allows Lance to filter out irrelevant rows at the storage level, avoiding reading them into memory or transferring them over the network.

### Supported Filters
The following conditions are translated and pushed down to the Lance storage layer:
* **Null Check:** `IS NULL` and `IS NOT NULL`
* **Comparisons:** `=`, `>`, `>=`, `<`, `<=`
* **Set Membership:** `IN (val1, val2, ...)` 
* **Supported Pushdown Data Types:** `BOOLEAN`, all integer types (`TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`), floating-point types (`REAL`, `DOUBLE`), `VARCHAR`, `DATE`, and `TIMESTAMP`.

### Session Property
By default, filter pushdown is enabled. You can toggle this behavior at session level:

```sql
-- Disable filter pushdown
SET SESSION lance.filter_pushdown_enabled = false;

-- Re-enable filter pushdown
SET SESSION lance.filter_pushdown_enabled = true;
```
