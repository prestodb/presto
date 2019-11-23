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

import com.facebook.presto.orc.OrcDataSink;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.filesystem.FileSystemContext;
import com.facebook.presto.raptor.filesystem.LocalOrcDataEnvironment;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.organization.ShardCompactor;
import com.facebook.presto.raptor.storage.organization.StagedShardCompactor.StagedCompactionStorageManager;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
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
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.filesystem.LocalOrcDataEnvironment.tryGetLocalFileSystem;
import static com.facebook.presto.raptor.storage.organization.StagedShardCompactor.SMALL_READ_ATTRIBUTES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StagedWriteStorageManager
        implements StorageManager
{
    private static final long MAX_ROWS = 1_000_000_000;

    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final TypeManager typeManager;
    private final ExecutorService commitExecutor;
    private final OrcDataEnvironment orcDataEnvironment = new LocalOrcDataEnvironment();
    private final OrcWriterStats stats = new OrcWriterStats();
    private final ShardCompactor shardCompactor;
    private final StagedCompactionStorageManager stagedCompactionStorageManager;
    private final File baseDirectory;

    @Inject
    public StagedWriteStorageManager(
            StorageManagerConfig config,
            RaptorConnectorId connectorId,
            TypeManager typeManager,
            ShardCompactor shardCompactor,
            StagedCompactionStorageManager stagedCompactionStorageManager)
    {
        this(
                typeManager,
                connectorId.toString(),
                config.getMaxShardRows(),
                config.getMaxShardSize(),
                config.getStagingWriteDirectory(),
                shardCompactor,
                stagedCompactionStorageManager);
    }

    public StagedWriteStorageManager(
            TypeManager typeManager,
            String connectorId,
            long maxShardRows,
            DataSize maxShardSize,
            URI baseDirectory,
            ShardCompactor shardCompactor,
            StagedCompactionStorageManager stagedCompactionStorageManager)
    {
        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = min(maxShardRows, MAX_ROWS);
        this.maxShardSize = requireNonNull(maxShardSize, "maxShardSize is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitExecutor = newCachedThreadPool(daemonThreadsNamed("raptor-commit-" + connectorId + "-%s"));
        this.baseDirectory = requireNonNull(tryGetLocalFileSystem(orcDataEnvironment).get().pathToFile(new Path(baseDirectory)), "dataDirectory is null");
        this.shardCompactor = requireNonNull(shardCompactor, "stagedShardCompactor is null");
        this.stagedCompactionStorageManager = requireNonNull(stagedCompactionStorageManager, "stagedCompactionStorageManager is null");
    }

    @PreDestroy
    public void shutdown()
    {
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
        throw new UnsupportedOperationException("no page source");
    }

    @Override
    public StoragePageSink createStoragePageSink(
            FileSystemContext fileSystemContext,
            long transactionId,
            OptionalInt bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            List<Long> sortFields,
            List<SortOrder> sortOrders,
            boolean checkSpace)
    {
        return new StagedWriteStoragePageSink(orcDataEnvironment.getFileSystem(fileSystemContext), transactionId, columnIds, columnTypes, bucketNumber, sortFields, sortOrders);
    }

    private class StagedWriteStoragePageSink
            implements StoragePageSink
    {
        private final long transactionId;
        private final List<Long> columnIds;
        private final List<Type> columnTypes;
        private final OptionalInt bucketNumber;
        private final List<Long> sortFields;
        private final List<SortOrder> sortOrders;

        private final List<Path> stagingFiles = new ArrayList<>();
        private final Set<UUID> stagingShards = new HashSet<>();
        private final List<ShardInfo> shards = new ArrayList<>();
        private final List<CompletableFuture<?>> futures = new ArrayList<>();
        private final FileSystem fileSystem;

        private long totalRows;
        private long totalCompressedBytes;

        private boolean committed;
        private FileWriter writer;
        private UUID shardUuid;

        public StagedWriteStoragePageSink(
                FileSystem fileSystem,
                long transactionId,
                List<Long> columnIds,
                List<Type> columnTypes,
                OptionalInt bucketNumber,
                List<Long> sortFields,
                List<SortOrder> sortOrders)
        {
            this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
            this.transactionId = transactionId;
            this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
            this.sortFields = requireNonNull(sortFields, "sortFields is null");
            this.sortOrders = requireNonNull(sortOrders, "sortOrders is null");
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
            flush(false);
        }

        public void flush(boolean force)
        {
            if (writer == null && stagingShards.isEmpty()) {
                return;
            }

            if (writer != null) {
                try {
                    writer.close();
                }
                catch (IOException e) {
                    throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
                }

                Path stagingFile = getStorageFile(shardUuid);
                stagingShards.add(shardUuid);
                try {
                    totalRows += writer.getRowCount();
                    totalCompressedBytes += fileSystem.getFileStatus(stagingFile).getLen();
                }
                catch (IOException e) {
                    throw new PrestoException(RAPTOR_ERROR, "Failed to get file status: " + stagingFile, e);
                }
            }

            writer = null;
            shardUuid = null;

            if (stagingShards.isEmpty()) {
                return;
            }

            List<ShardInfo> shardInfos;
            if (force || totalRows > maxShardRows || totalCompressedBytes > new DataSize(100, MEGABYTE).toBytes()) {
                try {
                    if (sortFields.isEmpty()) {
                        shardInfos = shardCompactor.compact(
                                stagedCompactionStorageManager,
                                SMALL_READ_ATTRIBUTES,
                                transactionId,
                                bucketNumber,
                                stagingShards,
                                columnIds,
                                columnTypes);
                    }
                    else {
                        List<ColumnInfo> columns = new ArrayList<>();
                        for (int i = 0; i < columnIds.size(); i++) {
                            columns.add(new ColumnInfo(columnIds.get(i), columnTypes.get(i)));
                        }

                        shardInfos = shardCompactor.compactSorted(
                                stagedCompactionStorageManager,
                                SMALL_READ_ATTRIBUTES,
                                transactionId,
                                bucketNumber,
                                stagingShards,
                                columns,
                                sortFields,
                                sortOrders);
                    }
                }
                catch (IOException e) {
                    throw new PrestoException(RAPTOR_ERROR, "Failed to flush writer", e);
                }

                shards.addAll(shardInfos);
                stagingShards.clear();
                totalRows = 0;
                totalCompressedBytes = 0;
            }
        }

        @Override
        public CompletableFuture<List<ShardInfo>> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush(true);

            for (Path file : stagingFiles) {
                try {
                    fileSystem.delete(file, false);
                }
                catch (IOException e) {
                    // ignore
                }
            }

            return allAsList(futures).thenApplyAsync(ignored -> ImmutableList.copyOf(shards), commitExecutor);
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
                for (Path file : stagingFiles) {
                    try {
                        fileSystem.delete(file, false);
                    }
                    catch (IOException e) {
                        // ignore
                    }
                }

                // cancel incomplete backup jobs
                futures.forEach(future -> future.cancel(true));
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                shardUuid = UUID.randomUUID();
                Path stagingFile = getStorageFile(shardUuid);
                stagingFiles.add(stagingFile);
                OrcDataSink sink;
                try {
                    sink = orcDataEnvironment.createOrcDataSink(fileSystem, stagingFile);
                }
                catch (IOException e) {
                    throw new PrestoException(RAPTOR_ERROR, format("Failed to create staging file %s", stagingFile), e);
                }
                writer = new OrcFileWriter(columnIds, columnTypes, sink, false, stats, typeManager, ZSTD);
            }
        }
    }

    private Path getStorageFile(UUID shardUuid)
    {
        return new Path(baseDirectory.toPath().resolve(shardUuid.toString() + ".staging").toFile().toString());
    }
}
