# Performance Tuning

The Presto LanceDB connector utilizes caching at multiple layers to deliver high-performance analytical queries. Tuning these caches and batch sizes is key to optimizing memory usage and request throughput.

## Caching Architecture

The connector maintains three cache categories per worker:

### 1. Dataset Cache
* **Property:** `lance.dataset-cache-max-entries` and `lance.dataset-cache-ttl`
* **Purpose:** Caches the opened Lance dataset objects to prevent constant filesystem metadata re-evaluation during concurrent reads.
* **Tuning:** If you query a large number of distinct tables within a short timeframe, increase the maximum entry limit (default `100`) to avoid thrashing.

### 2. Metadata Cache
* **Property:** `lance.metadata-cache-size`
* **Purpose:** Allocates a chunk of off-heap memory to cache Arrow/Lance metadata.
* **Tuning:** Increase this value (default `128MB`) if your tables have extremely wide schemas, thousands of columns, or nested structures.

### 3. Index Cache
* **Property:** `lance.index-cache-size`
* **Purpose:** Caches vector search indexes (such as IVF_PQ) during execution.
* **Tuning:** For heavily indexed vector tables, increase this size (default `128MB`) to keep indexes hot in RAM.

---

## Batch Sizing

### Read Batch Size (`lance.read-batch-size`)
* Default: `8192` rows.
* Determines how many records are returned in a single Arrow record batch. 
* **Tuning:** For wide tables, you can lower this value to reduce heap fragmentation. For narrow tables, increasing this value can improve throughput.

### Write Batch Size (`lance.write-batch-size`)
* Default: `10000` rows.
* The number of rows accumulated in Presto page memory before flushing them to Lance files.
* **Tuning:** Larger sizes lead to better compression and layout efficiency in the final files, but increase JVM heap usage during `INSERT` operations.
