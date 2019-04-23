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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.raptorx.RaptorColumnHandle;
import com.facebook.presto.raptorx.chunkstore.ChunkStore;
import com.facebook.presto.raptorx.chunkstore.ChunkStoreManager;
import com.facebook.presto.raptorx.metadata.ChunkRecorder;
import com.facebook.presto.raptorx.storage.OrcFileRewriter.OrcFileInfo;
import com.facebook.presto.raptorx.storage.organization.PageIndexInfo;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.raptorx.RaptorColumnHandle.isBucketNumberColumn;
import static com.facebook.presto.raptorx.RaptorColumnHandle.isChunkIdColumn;
import static com.facebook.presto.raptorx.RaptorColumnHandle.isChunkRowIdColumn;
import static com.facebook.presto.raptorx.RaptorColumnHandle.isHiddenColumn;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_CHUNK_STORE_ERROR;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_LOCAL_DISK_FULL;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_RECOVERY_TIMEOUT;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_STORAGE_ERROR;
import static com.facebook.presto.raptorx.storage.ColumnStatsBuilder.computeColumnStats;
import static com.facebook.presto.raptorx.storage.OrcFileRewriter.rewriteOrcFile;
import static com.facebook.presto.raptorx.storage.OrcPageSource.BUCKET_NUMBER_COLUMN;
import static com.facebook.presto.raptorx.storage.OrcPageSource.CHUNK_ID_COLUMN;
import static com.facebook.presto.raptorx.storage.OrcPageSource.NULL_COLUMN;
import static com.facebook.presto.raptorx.storage.OrcPageSource.ROW_ID_COLUMN;
import static com.facebook.presto.raptorx.util.Closeables.closeWithSuppression;
import static com.facebook.presto.raptorx.util.OrcUtil.createOrcReader;
import static com.facebook.presto.raptorx.util.OrcUtil.fileOrcDataSource;
import static com.facebook.presto.raptorx.util.StorageUtil.xxhash64;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.util.concurrent.Futures.whenAllSucceed;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.Math.min;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.joda.time.DateTimeZone.UTC;

public class OrcStorageManager
        implements StorageManager
{
    private static final Logger log = Logger.get(OrcStorageManager.class);

    private static final JsonCodec<ChunkDelta> CHUNK_DELTA_CODEC = jsonCodec(ChunkDelta.class);

    private static final long MAX_CHUNK_ROWS = 1_000_000_000;

    private final OrcWriterStats stats = new OrcWriterStats();
    private final String nodeIdentifier;
    private final StorageService storageService;
    private final ChunkStore chunkStore;
    private final ReaderAttributes defaultReaderAttributes;
    private final ChunkStoreManager chunkStoreManager;
    private final ChunkRecoveryManager recoveryManager;
    private final ChunkRecorder chunkRecorder;
    private final ChunkIdSequence chunkIdSequence;
    private final Duration recoveryTimeout;
    private final long maxChunkRows;
    private final DataSize maxChunkSize;
    private final DataSize minAvailableSpace;
    private final TypeManager typeManager;
    private final ExecutorService deletionExecutor;
    private final ExecutorService commitExecutor;

    @Inject
    public OrcStorageManager(
            NodeManager nodeManager,
            StorageService storageService,
            ChunkStore chunkStore,
            ReaderAttributes readerAttributes,
            StorageConfig config,
            ChunkStoreManager chunkStoreManager,
            ChunkRecoveryManager recoveryManager,
            ChunkRecorder chunkRecorder,
            ChunkIdSequence chunkIdSequence,
            TypeManager typeManager)
    {
        this(nodeManager.getCurrentNode().getNodeIdentifier(),
                storageService,
                chunkStore,
                readerAttributes,
                chunkStoreManager,
                recoveryManager,
                chunkRecorder,
                chunkIdSequence,
                typeManager,
                config.getDeletionThreads(),
                config.getChunkRecoveryTimeout(),
                config.getMaxChunkRows(),
                config.getMaxChunkSize(),
                config.getMinAvailableSpace());
    }

    public OrcStorageManager(
            String nodeIdentifier,
            StorageService storageService,
            ChunkStore chunkStore,
            ReaderAttributes readerAttributes,
            ChunkStoreManager chunkStoreManager,
            ChunkRecoveryManager recoveryManager,
            ChunkRecorder chunkRecorder,
            ChunkIdSequence chunkIdSequence,
            TypeManager typeManager,
            int deletionThreads,
            Duration chunkRecoveryTimeout,
            long maxChunkRows,
            DataSize maxChunkSize,
            DataSize minAvailableSpace)
    {
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeId is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.chunkStore = requireNonNull(chunkStore, "chunkStore is null");
        this.defaultReaderAttributes = requireNonNull(readerAttributes, "readerAttributes is null");

        this.chunkStoreManager = requireNonNull(chunkStoreManager, "chunkStoreManager is null");
        this.recoveryManager = requireNonNull(recoveryManager, "recoveryManager is null");
        this.recoveryTimeout = requireNonNull(chunkRecoveryTimeout, "chunkRecoveryTimeout is null");

        checkArgument(maxChunkRows > 0, "maxChunkRows must be greater than zero");
        this.maxChunkRows = min(maxChunkRows, MAX_CHUNK_ROWS);
        this.maxChunkSize = requireNonNull(maxChunkSize, "maxChunkSize is null");
        this.minAvailableSpace = requireNonNull(minAvailableSpace, "minAvailableSpace is null");
        this.chunkRecorder = requireNonNull(chunkRecorder, "chunkRecorder is null");
        this.chunkIdSequence = requireNonNull(chunkIdSequence, "chunkIdSequence is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.deletionExecutor = newFixedThreadPool(deletionThreads, daemonThreadsNamed("raptor-delete-%s"));
        this.commitExecutor = newCachedThreadPool(daemonThreadsNamed("raptor-commit-%s"));
    }

    @PreDestroy
    public void shutdown()
    {
        deletionExecutor.shutdownNow();
        commitExecutor.shutdownNow();
    }

    @Managed
    @Flatten
    public OrcWriterStats getStats()
    {
        return stats;
    }

    @Override
    public ConnectorPageSource getPageSource(
            long tableId,
            long chunkId,
            int bucketNumber,
            List<Long> columnIds,
            List<Type> columnTypes,
            TupleDomain<RaptorColumnHandle> effectivePredicate,
            ReaderAttributes attributes,
            OptionalLong transactionId,
            Map<Long, Type> chunkColumnTypes,
            Optional<CompressionType> compressionType)
    {
        OrcDataSource dataSource = openChunk(tableId, chunkId, attributes);

        try {
            OrcReader reader = createOrcReader(dataSource, attributes);

            if (OrcFileMetadata.from(reader).getChunkId() != chunkId) {
                throw new IOException("Wrong chunk ID for file: " + dataSource.getId());
            }

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
                    includedColumns.put(index, columnTypes.get(i));
                }
            }

            OrcPredicate predicate = getPredicate(effectivePredicate, indexMap);

            AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();

            OrcRecordReader recordReader = reader.createRecordReader(includedColumns.build(), predicate, UTC, systemMemoryUsage, MAX_BATCH_SIZE);

            Optional<ChunkRewriter> chunkRewriter = Optional.empty();
            if (transactionId.isPresent()) {
                checkArgument(!chunkColumnTypes.isEmpty(), "chunkColumnTypes is empty");
                checkArgument(compressionType.isPresent(), "compressionType not present");
                chunkRewriter = Optional.of(createChunkRewriter(transactionId.getAsLong(), tableId, bucketNumber, chunkId, chunkColumnTypes, compressionType.get()));
            }

            return new OrcPageSource(chunkRewriter, recordReader, dataSource, columnIds, columnTypes, columnIndexes.build(), chunkId, bucketNumber, systemMemoryUsage);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e, dataSource);
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to create page source for chunk " + chunkId, e);
        }
        catch (Throwable t) {
            closeWithSuppression(t, dataSource);
            throw t;
        }
    }

    private static int toSpecialIndex(long columnId)
    {
        if (isChunkRowIdColumn(columnId)) {
            return ROW_ID_COLUMN;
        }
        if (isChunkIdColumn(columnId)) {
            return CHUNK_ID_COLUMN;
        }
        if (isBucketNumberColumn(columnId)) {
            return BUCKET_NUMBER_COLUMN;
        }
        throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Invalid column ID: " + columnId);
    }

    @Override
    public StoragePageSink createStoragePageSink(long transactionId, long tableId, int bucketNumber, List<Long> columnIds, List<Type> columnTypes, CompressionType compressionType)
    {
        if (storageService.getAvailableBytes() < minAvailableSpace.toBytes()) {
            throw new PrestoException(RAPTOR_LOCAL_DISK_FULL, "Local disk is full on node " + nodeIdentifier);
        }
        return new OrcStoragePageSink(transactionId, tableId, bucketNumber, columnIds, columnTypes, compressionType);
    }

    private ChunkRewriter createChunkRewriter(long transactionId, long tableId, int bucketNumber, long chunkId, Map<Long, Type> chunkColumnTypes, CompressionType compressionType)
    {
        return rowsToDelete -> {
            if (rowsToDelete.isEmpty()) {
                return completedFuture(ImmutableList.of());
            }
            return supplyAsync(
                    () -> rewriteChunk(transactionId, tableId, bucketNumber, chunkId, chunkColumnTypes, compressionType, rowsToDelete),
                    deletionExecutor);
        };
    }

    private void writeChunk(long chunkId)
    {
        if (!chunkStore.chunkExists(chunkId)) {
            throw new PrestoException(RAPTOR_CHUNK_STORE_ERROR, "Chunk does not exist in chunk store after put");
        }

        File stagingFile = storageService.getStagingFile(chunkId);
        File storageFile = storageService.getStorageFile(chunkId);

        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to move chunk file", e);
        }
    }

    private OrcDataSource openChunk(long tableId, long chunkId, ReaderAttributes readerAttributes)
    {
        File file = storageService.getStorageFile(chunkId).getAbsoluteFile();
        log.info("tableID: %d, chunkId: %d, file: %s", tableId, chunkId, file.getAbsolutePath());
        if (!file.exists()) {
            try {
                Future<?> future = recoveryManager.recoverChunk(tableId, chunkId);
                future.get(recoveryTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                throwIfInstanceOf(e.getCause(), PrestoException.class);
                throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Error recovering chunk: " + chunkId, e.getCause());
            }
            catch (TimeoutException e) {
                throw new PrestoException(RAPTOR_RECOVERY_TIMEOUT, "Chunk is being recovered from chunk store. Please retry in a few minutes: " + chunkId);
            }
        }

        try {
            return fileOrcDataSource(file, readerAttributes);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to open chunk file: " + file, e);
        }
    }

    private ChunkInfo createChunkInfo(long chunkId, int bucketNumber, File file, long rowCount, long uncompressedSize)
    {
        return new ChunkInfo(chunkId, bucketNumber, computeChunkStats(file), rowCount, file.length(), uncompressedSize, xxhash64(file));
    }

    private List<ColumnStats> computeChunkStats(File file)
    {
        try (OrcDataSource dataSource = fileOrcDataSource(file, defaultReaderAttributes)) {
            OrcReader reader = createOrcReader(dataSource, defaultReaderAttributes);

            ImmutableList.Builder<ColumnStats> list = ImmutableList.builder();
            for (Entry<Long, Type> entry : getColumnTypes(reader).entrySet()) {
                computeColumnStats(reader, entry.getKey(), entry.getValue()).ifPresent(list::add);
            }
            return list.build();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to read file: " + file, e);
        }
    }

    private Collection<Slice> rewriteChunk(
            long transactionId,
            long tableId,
            int bucketNumber,
            long chunkId,
            Map<Long, Type> columnTypes,
            CompressionType compressionType,
            BitSet rowsToDelete)
    {
        if (rowsToDelete.isEmpty()) {
            return ImmutableList.of();
        }

        long newChunkId = chunkIdSequence.nextChunkId();
        File input = storageService.getStorageFile(chunkId);
        File output = storageService.getStagingFile(newChunkId);

        OrcFileInfo info = rewriteOrcFile(
                newChunkId,
                input,
                output,
                columnTypes,
                compressionType,
                rowsToDelete,
                defaultReaderAttributes,
                typeManager,
                stats);

        long rowCount = info.getRowCount();

        if (rowCount == 0) {
            return chunkDelta(chunkId, Optional.empty());
        }

        chunkRecorder.recordCreatedChunk(transactionId, tableId, newChunkId, output.length());

        // submit operation and wait until it finishes
        getFutureValue(chunkStoreManager.submit(newChunkId, output));

        long uncompressedSize = info.getUncompressedSize();

        ChunkInfo chunk = createChunkInfo(newChunkId, bucketNumber, output, rowCount, uncompressedSize);

        writeChunk(newChunkId);

        return chunkDelta(chunkId, Optional.of(chunk));
    }

    private static Collection<Slice> chunkDelta(long oldChunkId, Optional<ChunkInfo> chunkInfo)
    {
        List<ChunkInfo> newChunks = chunkInfo.map(ImmutableList::of).orElse(ImmutableList.of());
        ChunkDelta delta = new ChunkDelta(ImmutableSet.of(oldChunkId), newChunks);
        return ImmutableList.of(Slices.wrappedBuffer(CHUNK_DELTA_CODEC.toJsonBytes(delta)));
    }

    private Map<Long, Type> getColumnTypes(OrcReader reader)
    {
        return ImmutableMap.copyOf(transformValues(OrcFileMetadata.from(reader).getColumnTypes(), typeManager::getType));
    }

    private static OrcPredicate getPredicate(TupleDomain<RaptorColumnHandle> effectivePredicate, Map<Long, Integer> indexMap)
    {
        Map<RaptorColumnHandle, Domain> domains = effectivePredicate.getDomains()
                .orElseThrow(() -> new PrestoException(RAPTOR_INTERNAL_ERROR, "Domain not set"));
        ImmutableList.Builder<ColumnReference<RaptorColumnHandle>> columns = ImmutableList.builder();
        for (RaptorColumnHandle column : domains.keySet()) {
            Integer index = indexMap.get(column.getColumnId());
            if (index != null) {
                columns.add(new ColumnReference<>(column, index, column.getColumnType()));
            }
        }
        return new TupleDomainOrcPredicate<>(effectivePredicate, columns.build(), false);
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
        private final long transactionId;
        private final long tableId;
        private final int bucketNumber;
        private final List<Long> columnIds;
        private final List<Type> columnTypes;
        private final CompressionType compressionType;

        private final List<File> stagingFiles = new ArrayList<>();
        private final List<ChunkInfo> chunks = new ArrayList<>();
        private final List<ListenableFuture<?>> chunkStoreJobs = new ArrayList<>();

        private boolean committed;
        private OrcFileWriter writer;
        private long chunkId;

        public OrcStoragePageSink(
                long transactionId,
                long tableId,
                int bucketNumber,
                List<Long> columnIds,
                List<Type> columnTypes,
                CompressionType compressionType)
        {
            this.transactionId = transactionId;
            this.tableId = tableId;
            this.bucketNumber = bucketNumber;
            this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.compressionType = requireNonNull(compressionType, "compressionType is null");
        }

        @Override
        public void appendPages(List<Page> pages)
        {
            createWriterIfNecessary();
            writer.appendPages(pages);
        }

        @Override
        public void appendPages(List<Page> pages, int[] pageIndexes, int[] positionIndexes)
        {
            createWriterIfNecessary();
            writer.appendPages(pages, pageIndexes, positionIndexes);
        }

        @Override
        public void appendPageIndexInfos(List<PageIndexInfo> pageIndexInfo)
        {
            createWriterIfNecessary();
            writer.appendPageIndexInfos(pageIndexInfo);
        }

        @Override
        public boolean isFull()
        {
            if (writer == null) {
                return false;
            }
            return (writer.getRowCount() >= maxChunkRows) || (writer.getUncompressedSize() >= maxChunkSize.toBytes());
        }

        @Override
        public void flush()
        {
            if (writer != null) {
                writer.close();

                File file = storageService.getStagingFile(chunkId);

                chunkRecorder.recordCreatedChunk(transactionId, tableId, chunkId, file.length());

                chunkStoreJobs.add(chunkStoreManager.submit(chunkId, file));

                long rowCount = writer.getRowCount();
                long uncompressedSize = writer.getUncompressedSize();

                chunks.add(createChunkInfo(chunkId, bucketNumber, file, rowCount, uncompressedSize));

                writer = null;
                chunkId = 0;
            }
        }

        @Override
        public ListenableFuture<List<ChunkInfo>> commit()
        {
            checkState(!committed, "already committed");
            committed = true;

            flush();

            return whenAllSucceed(chunkStoreJobs).call(() -> {
                for (ChunkInfo chunk : chunks) {
                    writeChunk(chunk.getChunkId());
                }
                return ImmutableList.copyOf(chunks);
            }, commitExecutor);
        }

        @SuppressWarnings("ResultOfMethodCallIgnored")
        @Override
        public void rollback()
        {
            try {
                if (writer != null) {
                    writer.close();
                    writer = null;
                }
            }
            finally {
                for (File file : stagingFiles) {
                    file.delete();
                }

                // cancel incomplete jobs
                chunkStoreJobs.forEach(future -> future.cancel(true));

                // try to delete completed chunks
                for (ChunkInfo chunk : chunks) {
                    chunkStore.deleteChunk(chunk.getChunkId());
                }
            }
        }

        private void createWriterIfNecessary()
        {
            if (writer == null) {
                chunkId = chunkIdSequence.nextChunkId();
                File stagingFile = storageService.getStagingFile(chunkId);
                stagingFiles.add(stagingFile);
                writer = new OrcFileWriter(chunkId, columnIds, columnTypes, compressionType, stagingFile, typeManager, stats);
            }
        }
    }
}
