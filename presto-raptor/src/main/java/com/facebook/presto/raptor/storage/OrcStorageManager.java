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

import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.raptor.util.PageBuffer;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.facebook.presto.raptor.storage.ShardStats.computeColumnStats;
import static com.facebook.presto.raptor.util.FileUtil.copyFile;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.joda.time.DateTimeZone.UTC;

public class OrcStorageManager
        implements StorageManager
{
    private final String nodeId;
    private final StorageService storageService;
    private final DataSize orcMaxMergeDistance;
    private final DataSize orcMaxReadSize;
    private final DataSize orcStreamBufferSize;
    private final ShardRecoveryManager recoveryManager;
    private final Duration recoveryTimeout;
    private final long maxShardRows;
    private final DataSize maxShardSize;
    private final DataSize maxBufferSize;
    private final StorageManagerStats stats;

    @Inject
    public OrcStorageManager(
            CurrentNodeId currentNodeId,
            StorageService storageService,
            StorageManagerConfig config,
            ShardRecoveryManager recoveryManager)
    {
        this(currentNodeId.toString(),
                storageService,
                config.getOrcMaxMergeDistance(),
                config.getOrcMaxReadSize(),
                config.getOrcStreamBufferSize(),
                recoveryManager,
                config.getShardRecoveryTimeout(),
                config.getMaxShardRows(),
                config.getMaxShardSize(),
                config.getMaxBufferSize());
    }

    public OrcStorageManager(
            String nodeId,
            StorageService storageService,
            DataSize orcMaxMergeDistance,
            DataSize orcMaxReadSize,
            DataSize orcStreamBufferSize,
            ShardRecoveryManager recoveryManager,
            Duration shardRecoveryTimeout,
            long maxShardRows,
            DataSize maxShardSize,
            DataSize maxBufferSize)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.storageService = checkNotNull(storageService, "storageService is null");
        this.orcMaxMergeDistance = checkNotNull(orcMaxMergeDistance, "orcMaxMergeDistance is null");
        this.orcMaxReadSize = checkNotNull(orcMaxReadSize, "orcMaxReadSize is null");
        this.orcStreamBufferSize = checkNotNull(orcStreamBufferSize, "orcStreamBufferSize is null");

        this.recoveryManager = checkNotNull(recoveryManager, "recoveryManager is null");
        this.recoveryTimeout = checkNotNull(shardRecoveryTimeout, "shardRecoveryTimeout is null");

        checkArgument(maxShardRows > 0, "maxShardRows must be > 0");
        this.maxShardRows = maxShardRows;
        this.maxShardSize = checkNotNull(maxShardSize, "maxShardSize is null");
        this.maxBufferSize = checkNotNull(maxBufferSize, "maxBufferSize is null");
        this.stats = new StorageManagerStats();
    }

    @Override
    public ConnectorPageSource getPageSource(UUID shardUuid, List<Long> columnIds, List<Type> columnTypes, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        OrcDataSource dataSource = openShard(shardUuid);

        try {
            OrcReader reader = new OrcReader(dataSource, new OrcMetadataReader());

            Map<Long, Integer> indexMap = columnIdIndex(reader.getColumnNames());
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<Integer> columnIndexes = ImmutableList.builder();
            for (int i = 0; i < columnIds.size(); i++) {
                long columnId = columnIds.get(i);
                Integer index = indexMap.get(columnId);
                if (index == null) {
                    columnIndexes.add(-1);
                }
                else {
                    columnIndexes.add(index);
                    includedColumns.put(index, columnTypes.get(i));
                }
            }

            OrcPredicate predicate = getPredicate(effectivePredicate, indexMap);

            OrcRecordReader recordReader = reader.createRecordReader(includedColumns.build(), predicate, UTC);

            return new OrcPageSource(recordReader, dataSource, columnIds, columnTypes, columnIndexes.build());
        }
        catch (IOException | RuntimeException e) {
            try {
                dataSource.close();
            }
            catch (IOException ex) {
                e.addSuppressed(ex);
            }
            throw new PrestoException(RAPTOR_ERROR, "Failed to create page source for shard " + shardUuid, e);
        }
    }

    @Override
    public StoragePageSink createStoragePageSink(List<Long> columnIds, List<Type> columnTypes)
    {
        return new OrcStoragePageSink(columnIds, columnTypes, maxShardRows, maxShardSize);
    }

    private void writeShard(UUID shardUuid)
    {
        File stagingFile = storageService.getStagingFile(shardUuid);
        File storageFile = storageService.getStorageFile(shardUuid);

        storageService.createParents(storageFile);

        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to move shard file", e);
        }

        if (isBackupAvailable()) {
            File backupFile = storageService.getBackupFile(shardUuid);
            long start = System.nanoTime();
            storageService.createParents(backupFile);
            stats.addCreateParentsTime(Duration.nanosSince(start));

            try {
                start = System.nanoTime();
                copyFile(storageFile.toPath(), backupFile.toPath());
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, "Failed to create backup shard file", e);
            }
            stats.addCopyShardDataRate(new DataSize(storageFile.length(), BYTE), nanosSince(start));
        }
    }

    @Override
    public PageBuffer createPageBuffer()
    {
        return new PageBuffer(maxBufferSize.toBytes(), Integer.MAX_VALUE);
    }

    @Override
    public boolean isBackupAvailable()
    {
        return storageService.isBackupAvailable();
    }

    @VisibleForTesting
    OrcDataSource openShard(UUID shardUuid)
    {
        File file = storageService.getStorageFile(shardUuid).getAbsoluteFile();

        if (!file.exists() && storageService.isBackupAvailable(shardUuid)) {
            try {
                Future<?> future = recoveryManager.recoverShard(shardUuid);
                future.get(recoveryTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
                throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Error recovering shard " + shardUuid, e.getCause());
            }
            catch (TimeoutException e) {
                throw new PrestoException(RAPTOR_ERROR, "Shard is being recovered from backup. Please retry in a few minutes: " + shardUuid);
            }
        }

        try {
            return new FileOrcDataSource(file, orcMaxMergeDistance, orcMaxMergeDistance, orcMaxMergeDistance);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to open shard file: " + file, e);
        }
    }

    private List<ColumnStats> computeShardStats(File file, List<Long> columnIds, List<Type> types)
    {
        try (OrcDataSource dataSource = new FileOrcDataSource(file, orcMaxMergeDistance, orcMaxReadSize, orcStreamBufferSize)) {
            OrcReader reader = new OrcReader(dataSource, new OrcMetadataReader());

            ImmutableList.Builder<ColumnStats> list = ImmutableList.builder();
            for (int i = 0; i < columnIds.size(); i++) {
                computeColumnStats(reader, columnIds.get(i), types.get(i)).ifPresent(list::add);
            }
            return list.build();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to read file: " + file, e);
        }
    }

    private static OrcPredicate getPredicate(TupleDomain<RaptorColumnHandle> effectivePredicate, Map<Long, Integer> indexMap)
    {
        ImmutableList.Builder<ColumnReference<RaptorColumnHandle>> columns = ImmutableList.builder();
        for (RaptorColumnHandle column : effectivePredicate.getDomains().keySet()) {
            Integer index = indexMap.get(column.getColumnId());
            if (index != null) {
                columns.add(new ColumnReference<>(column, index, column.getColumnType()));
            }
        }
        return new TupleDomainOrcPredicate<>(effectivePredicate, columns.build());
    }

    private static Map<Long, Integer> columnIdIndex(List<String> columnNames)
    {
        ImmutableMap.Builder<Long, Integer> map = ImmutableMap.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(Long.valueOf(columnNames.get(i)), i);
        }
        return map.build();
    }

    private class OrcStoragePageSink
            implements StoragePageSink
    {
        private final List<Long> columnIds;
        private final List<Type> columnTypes;

        private final List<ShardInfo> shards = new ArrayList<>();
        private final long maxShardRows;
        private final DataSize maxShardSize;

        private boolean committed;
        private OrcFileWriter writer;
        private UUID shardUuid;

        public OrcStoragePageSink(List<Long> columnIds, List<Type> columnTypes, long maxShardRows, DataSize maxShardSize)
        {
            this.maxShardRows = maxShardRows;
            this.maxShardSize = maxShardSize;
            this.columnIds = ImmutableList.copyOf(checkNotNull(columnIds, "columnIds is null"));
            this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
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
                writer.close();

                File stagingFile = storageService.getStagingFile(shardUuid);
                List<ColumnStats> columns = computeShardStats(stagingFile, columnIds, columnTypes);
                Set<String> nodes = ImmutableSet.of(nodeId);
                long rowCount = writer.getRowCount();
                long dataSize = stagingFile.length();  // compressed size

                shards.add(new ShardInfo(shardUuid, nodes, columns, rowCount, dataSize));

                writer = null;
                shardUuid = null;
            }
        }

        @Override
        public List<ShardInfo> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush();
            for (ShardInfo shard : shards) {
                writeShard(shard.getShardUuid());
            }
            return ImmutableList.copyOf(shards);
        }

        @Override
        public void rollback()
        {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                shardUuid = UUID.randomUUID();
                File stagingFile = storageService.getStagingFile(shardUuid);
                storageService.createParents(stagingFile);
                writer = new OrcFileWriter(columnIds, columnTypes, stagingFile);
            }
        }
    }

    @Managed
    @Flatten
    public StorageManagerStats getStats()
    {
        return stats;
    }
}
