Velox emits several metrics during query execution which are useful to analyze the details of the underlying system and query processing. Some of these include: 

queryThreadIoLatency: Time spent by a query processing thread waiting for synchronously issued IO or for an in-progress read-ahead to finish.

numRamRead: Number of hits from RAM cache. Does not include first use of prefetched data.

numPrefetch: Number of planned read from storage or SSD.

prefetchBytes: Planned read from storage or SSD in bytes.

numStorageRead: Number of reads from storage, for sparsely accessed columns.

storageReadBytes: Bytes read from storage, for sparsely accessed columns.

numLocalRead: Number of reads from SSD cache instead of storage. Includes both random and planned reads.

localReadBytes: Bytes read from SSD cache instead of storage. Includes both random and planned reads.

numRamRead: Number of hits from RAM cache. Does not include first use of prefetched data.

ramReadBytes: Hits from RAM cache in bytes. Does not include first use of prefetched data.
