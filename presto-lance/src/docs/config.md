# Configuration

To configure the Presto LanceDB connector, create a catalog properties file `etc/catalog/lance.properties` inside your Presto server's configuration directory.

Here is a minimal configuration example:

```ini
connector.name=lance
lance.root-url=/Users/saurabhmahawar/Desktop/presto-server-0.297/lance-data
```

## Configuration Properties

The following configuration properties are available:

| Property Name | Description | Default Value | Required |
|---|---|---|---|
| `lance.root-url` | The root filesystem path where LanceDB tables are stored. Use absolute paths (e.g. `/path/to/data` or `file:///path/to/data`). | None | Yes |
| `lance.impl` | The namespace manager implementation to use (`dir` for local directories). | `dir` | No |
| `lance.single-level-ns` | If `true`, the connector registers a virtual schema named `default` to query flat folders directly inside the root URL. | `true` | No |
| `lance.read-batch-size` | The number of rows to retrieve in a single batch during reads. | `8192` | No |
| `lance.write-batch-size` | The number of rows to batch in memory before flushing to the Lance file. | `10000` | No |
| `lance.max-rows-per-file` | The maximum number of rows to write per output Lance file. | `1000000` | No |
| `lance.max-rows-per-group` | The maximum number of rows per row group inside the output Lance file. | `100000` | No |
| `lance.index-cache-size` | Size of the Lance index cache per Presto worker. | `128MB` | No |
| `lance.metadata-cache-size` | Size of the Lance metadata cache per Presto worker. | `128MB` | No |
| `lance.dataset-cache-max-entries` | The maximum number of cached Lance dataset handles per worker. | `100` | No |
| `lance.dataset-cache-ttl` | The duration for which cached Lance dataset handles remain valid. | `60m` | No |

## Schema Namespaces

Presto enforces a 3-part naming hierarchy (`catalog.schema.table`), while LanceDB handles datasets flatly in the root filesystem. 
* By default, `lance.single-level-ns` is set to `true`.
* Under this mode, the connector maps all Lance directories (ending with `.lance`) in the `lance.root-url` directory to a single virtual schema called `default`.
* Custom schemas cannot be created dynamically. Therefore, you must use the `default` schema when writing and querying tables (e.g., `lance.default.my_table`).
