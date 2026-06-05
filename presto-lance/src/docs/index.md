# Presto LanceDB Connector

The Presto LanceDB connector allows querying and writing LanceDB datasets from Presto. 

[LanceDB](https://github.com/lancedb/lancedb) is an open-source database for vector search built on top of the [Lance](https://github.com/lancedb/lance) columnar data format. Lance is designed for high-performance ML workloads, supporting fast random access, vector search, and versioned datasets.

## Key Features

- **High-Performance Columnar Reads:** Direct integration with Lance's C++ library via Arrow Dataset API.
- **SQL Operations:** Support for `CREATE TABLE`, `DROP TABLE`, `DESCRIBE`, `SELECT` queries, and `INSERT INTO` operations.
- **Predicate Pushdown:** Translates Presto query filters into Lance scan filters to minimize disk I/O and CPU overhead.
- **Snapshot Isolation:** Queries are executed against a consistent version of the dataset at the start of the transaction.
- **Worker-Level Caching:** Integrated caching for indexes, metadata, and dataset objects to minimize remote storage latency.
