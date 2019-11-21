/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor.storage;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSink;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.StripeMetadataSource;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.filesystem.FileSystemContext;
import com.facebook.presto.raptor.filesystem.LocalOrcDataEnvironment;
import com.facebook.presto.raptor.filesystem.RaptorLocalFileSystem;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.allAsList;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcPredicate.TRUE;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.raptor.RaptorColumnHandle.isHiddenColumn;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_FILE_SYSTEM_ERROR;
import static com.facebook.presto.raptor.storage.OrcPageSource.NULL_COLUMN;
import static com.facebook.presto.raptor.storage.StorageManagerUtils.DEFAULT_STORAGE_TIMEZONE;
import static com.facebook.presto.raptor.storage.StorageManagerUtils.HUGE_MAX_READ_BLOCK_SIZE;
import static com.facebook.presto.raptor.storage.StorageManagerUtils.toOrcFileType;
import static com.facebook.presto.raptor.storage.StorageManagerUtils.toSpecialIndex;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class StagingStorageManager
        implements StorageManager
{
    private static final long MAX_ROWS = 1_000_000_000;

    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final TypeManager typeManager;
    private final ExecutorService deletionExecutor;
    private final ExecutorService commitExecutor;
    private final OrcDataEnvironment orcDataEnvironment = new LocalOrcDataEnvironment();
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSource stripeMetadataSource;
    private final RawLocalFileSystem fileSystem;
    private final File baseDirectory;

    @Inject
    public StagingStorageManager(
            StorageManagerConfig config,
            RaptorConnectorId connectorId,
            TypeManager typeManager,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSource stripeMetadataSource)
    {
        this(
                typeManager,
                connectorId.toString(),
                config.getDeletionThreads(),
                config.getMaxShardRows(),
                config.getMaxShardSize(),
                config.getStagingWriteDirectory(),
                orcFileTailSource,
                stripeMetadataSource);
    }

    public StagingStorageManager(
            TypeManager typeManager,
            String connectorId,
            int deletionThreads,
            long maxShardRows,
            DataSize maxShardSize,
            URI baseDirectory,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSource stripeMetadataSource)
    {
        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(maxShardRows, MAX_ROWS);
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.deletionExecutor = newFixedThreadPool(deletionThreads, daemonThreadsNamed("raptor-delete-" + connectorId + "-%s"));
        this.commitExecutor = newCachedThreadPool(daemonThreadsNamed("raptor-commit-" + connectorId + "-%s"));
        try {
            this.fileSystem = new RaptorLocalFileSystem(new Configuration());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_FILE_SYSTEM_ERROR, e);
        }
        this.baseDirectory = requireNonNull(fileSystem.pathToFile(new Path(baseDirectory)), "dataDirectory is null");
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
        this.stripeMetadataSource = requireNonNull(stripeMetadataSource, "stripeMetadataSource is null");

        File root = new File(baseDirectory);
        if (!root.exists()) {
            try {
                Files.createDirectories(root.toPath());
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_FILE_SYSTEM_ERROR, "cannot create cache directory " + root, e);
            }
        }
        else {
            File[] files = root.listFiles();
            if (files == null) {
                return;
            }

            this.deletionExecutor.submit(() -> Arrays.stream(files).forEach(file -> {
                try {
                    Files.delete(file.toPath());
                }
                catch (IOException e) {
                    // ignore
                }
            }));
        }
    }

    @PreDestroy
    public void shutdown()
    {
        deletionExecutor.shutdownNow();
        commitExecutor.shutdown();
    }

    @Override
    public ConnectorPageSource getPageSource(
            FileSystemContext fileSystemContext,
            UUID shardUuid,
            OptionalInt bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            ReaderAttributes readerAttributes,
            OptionalLong transactionId,
            Optional<Map<String, Type>> allColumnTypes)
    {
        OrcDataSource dataSource = openShard(fileSystem, shardUuid, readerAttributes);

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();

        try {
            OrcReader reader = new OrcReader(
                    dataSource,
                    ORC,
                    orcFileTailSource,
                    stripeMetadataSource,
                    new OrcReaderOptions(readerAttributes.getMaxMergeDistance(), readerAttributes.getTinyStripeThreshold(), HUGE_MAX_READ_BLOCK_SIZE, readerAttributes.isZstdJniDecompressionEnabled()));

            Map<Long, Integer> indexMap = columnIdIndex(reader.getColumnNames());
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<Integer> columnIndexes = ImmutableList.builder();
            for (int i = 0; i < columnIds.size(); i++) {
                long columnId = columnIds.get(i);
                if (isHiddenColumn(columnId)) {
                    columnIndexes.add(toSpecialIndex(columnId));
                    continue;
                }

                Integer index = indexMap.get(columnId);
                if (index == null) {
                    columnIndexes.add(NULL_COLUMN);
                }
                else {
                    columnIndexes.add(index);
                    includedColumns.put(index, toOrcFileType(columnTypes.get(i), typeManager));
                }
            }

            StorageTypeConverter storageTypeConverter = new StorageTypeConverter(typeManager);

            OrcBatchRecordReader recordReader = reader.createBatchRecordReader(storageTypeConverter.toStorageTypes(includedColumns.build()), TRUE, DEFAULT_STORAGE_TIMEZONE, systemMemoryUsage, INITIAL_BATCH_SIZE);

            return new OrcPageSource(Optional.empty(), recordReader, dataSource, columnIds, columnTypes, columnIndexes.build(), shardUuid, bucketNumber, systemMemoryUsage);
        }
        catch (IOException | RuntimeException e) {
            closeQuietly(dataSource);
            throw new PrestoException(RAPTOR_ERROR, "Failed to create page source for shard " + shardUuid, e);
        }
        catch (Throwable t) {
            closeQuietly(dataSource);
            throw t;
        }
    }

    @Override
    public StoragePageSink createStoragePageSink(
            FileSystemContext fileSystemContext,
            long transactionId,
            OptionalInt bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            boolean checkSpace)
    {
        throw new UnsupportedOperationException("TODO");
    }

    public StoragePageSink createStoragePageSink(List<Long> columnIds, List<Type> columnTypes, UUID uuid)
    {
        return new StagingStoragePageSink(columnIds, columnTypes, uuid);
    }

    public void deleteStagingShards(Set<UUID> shards)
    {
        for (UUID shard : shards) {
            Path file = getStorageFile(shard);
            deletionExecutor.submit(() -> {
                try {
                    fileSystem.delete(file, false);
                }
                catch (IOException e) {
                    // ignore
                }
            });
        }
    }

    private OrcDataSource openShard(FileSystem fileSystem, UUID shardUuid, ReaderAttributes readerAttributes)
    {
        Path file = getStorageFile(shardUuid);

        try {
            return orcDataEnvironment.createOrcDataSource(fileSystem, file, readerAttributes);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to open shard file: " + file, e);
        }
    }

    private static Map<Long, Integer> columnIdIndex(List<String> columnNames)
    {
        ImmutableMap.Builder<Long, Integer> map = ImmutableMap.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(Long.valueOf(columnNames.get(i)), i);
        }
        return map.build();
    }

    private class StagingStoragePageSink
            implements StoragePageSink
    {
        private final List<Long> columnIds;
        private final List<Type> columnTypes;
        private final UUID shardUuid;

        private final List<CompletableFuture<?>> futures = new ArrayList<>();

        private boolean committed;
        private FileWriter writer;

        public StagingStoragePageSink(List<Long> columnIds, List<Type> columnTypes, UUID uuid)
        {
            this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.shardUuid = requireNonNull(uuid, "uuid is null");
        }

        @Override
        public void appendPages(List<Page> pages)
        {
            createWriterIfNecessary();
            writer.appendPages(pages);
        }

        @Override
        public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
        {
            createWriterIfNecessary();
            writer.appendPages(inputPages, pageIndexes, positionIndexes);
        }

        @Override
        public boolean isFull()
        {
            if (writer == null) {
                return false;
            }
            return (writer.getRowCount() >= maxShardRows) || (writer.getUncompressedSize() >= maxShardSize.toBytes());
        }

        @Override
        public void flush()
        {
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (IOException e) {
                    throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
                }

                writer = null;
            }
        }

        @Override
        public CompletableFuture<List<ShardInfo>> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush();

            return allAsList(futures).thenApplyAsync(ignored -> ImmutableList.of(), commitExecutor);
        }

        @Override
        public void rollback()
        {
            try {
                if (writer != null) {
                    try {
                        writer.close();
                    }
                    catch (IOException e) {
                        throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
                    }
                    finally {
                        writer = null;
                    }
                }
            }
            finally {
                try {
                    fileSystem.delete(getStorageFile(shardUuid), false);
                }
                catch (IOException e) {
                    // ignore
                }

                // cancel incomplete backup jobs
                futures.forEach(future -> future.cancel(true));
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                Path stagingFile = getStorageFile(shardUuid);
                OrcDataSink sink;
                try {
                    sink = orcDataEnvironment.createOrcDataSink(fileSystem, stagingFile);
                }
                catch (IOException e) {
                    throw new PrestoException(RAPTOR_ERROR, format("Failed to create staging file %s", stagingFile), e);
                }
                writer = new OrcFileWriter(columnIds, columnTypes, sink, false, new OrcWriterStats(), typeManager, ZSTD);
            }
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            closeable.close();
        }
        catch (IOException ignored) {
        }
    }

    private Path getStorageFile(UUID shardUuid)
    {
        return new Path(baseDirectory.toPath().resolve(shardUuid.toString() + ".staging").toFile().toString());
    }
}
