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
package com.facebook.presto.raptor.storage.organization;

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
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.FileWriter;
import com.facebook.presto.raptor.storage.OrcDataEnvironment;
import com.facebook.presto.raptor.storage.OrcFileWriter;
import com.facebook.presto.raptor.storage.ReaderAttributes;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.raptor.storage.StorageManagerUtils;
import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

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
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_FILE_SYSTEM_ERROR;
import static com.facebook.presto.raptor.filesystem.LocalOrcDataEnvironment.tryGetLocalFileSystem;
import static com.facebook.presto.raptor.storage.StorageManagerUtils.HUGE_MAX_READ_BLOCK_SIZE;
import static com.facebook.presto.raptor.storage.StorageManagerUtils.closeQuietly;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;

public final class StagedShardCompactor
        extends ShardCompactor
{
    private static final ReaderAttributes SMALL_READ_ATTRIBUTES = new ReaderAttributes(
            new DataSize(1, BYTE),
            new DataSize(100, KILOBYTE),
            new DataSize(100, KILOBYTE),
            new DataSize(1, BYTE),
            false,
            false);

    private final StorageManager orcStorageManager;
    private final StagedCompactionStorageManager stagedCompactionStorageManager;
    private final ReaderAttributes readerAttributes;

    @Inject
    public StagedShardCompactor(StorageManager orcStorageManager, StagedCompactionStorageManager stagedCompactionStorageManager, ReaderAttributes readerAttributes)
    {
        super(orcStorageManager, readerAttributes);
        this.orcStorageManager = requireNonNull(orcStorageManager, "storageManager is null");
        this.stagedCompactionStorageManager = requireNonNull(stagedCompactionStorageManager, "storageManager is null");
        this.readerAttributes = requireNonNull(readerAttributes, "readerAttributes is null");
    }

    @Override
    public List<ShardInfo> compactSorted(long transactionId, OptionalInt bucketNumber, Set<UUID> uuids, List<ColumnInfo> columns, List<Long> sortColumnIds, List<SortOrder> sortOrders)
            throws IOException
    {
        checkArgument(sortColumnIds.size() == sortOrders.size(), "sortColumnIds and sortOrders must be of the same size");

        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());

        checkArgument(columnIds.containsAll(sortColumnIds), "sortColumnIds must be a subset of columnIds");

        // staged compaction
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = orcStorageManager.getPageSource(FileSystemContext.DEFAULT_RAPTOR_CONTEXT, uuid, bucketNumber, columnIds, columnTypes, TupleDomain.all(), readerAttributes)) {
                StoragePageSink storagePageSink = null;
                try {
                    storagePageSink = stagedCompactionStorageManager.createStoragePageSink(columnIds, columnTypes, uuid);
                    while (!pageSource.isFinished()) {
                        Page page = pageSource.getNextPage();
                        if (isNullOrEmptyPage(page)) {
                            continue;
                        }
                        storagePageSink.appendPages(ImmutableList.of(page));
                    }
                    storagePageSink.commit();
                }
                catch (RuntimeException e) {
                    if (storagePageSink != null) {
                        storagePageSink.rollback();
                    }
                    throw e;
                }
            }
        }

        List<ShardInfo> shardInfos = compactSorted(stagedCompactionStorageManager, SMALL_READ_ATTRIBUTES, transactionId, bucketNumber, uuids, columns, sortColumnIds, sortOrders);

        stagedCompactionStorageManager.deleteStagingShards(uuids);

        return shardInfos;
    }

    public static class StagedCompactionStorageManager
            implements StorageManager
    {
        private final TypeManager typeManager;
        private final ExecutorService deletionExecutor;
        private final ExecutorService commitExecutor;
        private final OrcDataEnvironment orcDataEnvironment = new LocalOrcDataEnvironment();
        private final OrcFileTailSource orcFileTailSource;
        private final StripeMetadataSource stripeMetadataSource;
        private final RawLocalFileSystem fileSystem = tryGetLocalFileSystem(orcDataEnvironment).get();
        private final File baseDirectory;

        @Inject
        public StagedCompactionStorageManager(
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
                    config.getStagingWriteDirectory(),
                    orcFileTailSource,
                    stripeMetadataSource);
        }

        public StagedCompactionStorageManager(
                TypeManager typeManager,
                String connectorId,
                int deletionThreads,
                URI baseDirectory,
                OrcFileTailSource orcFileTailSource,
                StripeMetadataSource stripeMetadataSource)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.deletionExecutor = newFixedThreadPool(deletionThreads, daemonThreadsNamed("raptor-delete-" + connectorId + "-%s"));
            this.commitExecutor = newCachedThreadPool(daemonThreadsNamed("raptor-commit-" + connectorId + "-%s"));
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
            OrcDataSource dataSource;
            Path file = getStorageFile(shardUuid);
            try {
                dataSource = orcDataEnvironment.createOrcDataSource(fileSystem, file, readerAttributes);
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, "Failed to open shard file: " + file, e);
            }

            try {
                OrcReader reader = new OrcReader(
                        dataSource,
                        ORC,
                        orcFileTailSource,
                        stripeMetadataSource,
                        new OrcReaderOptions(readerAttributes.getMaxMergeDistance(), readerAttributes.getTinyStripeThreshold(), HUGE_MAX_READ_BLOCK_SIZE, readerAttributes.isZstdJniDecompressionEnabled()));

                return StorageManagerUtils.getPageSource(
                        reader,
                        dataSource,
                        typeManager,
                        shardUuid,
                        bucketNumber,
                        columnIds,
                        columnTypes,
                        TupleDomain.all(),
                        Optional.empty());
            }
            catch (IOException | RuntimeException e) {
                closeQuietly(dataSource);
                throw new PrestoException(RAPTOR_ERROR, "Failed to create page source for shard " + shardUuid, e);
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
                return false;
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

        private Path getStorageFile(UUID shardUuid)
        {
            return new Path(baseDirectory.toPath().resolve(shardUuid.toString() + ".staging").toFile().toString());
        }
    }
}
