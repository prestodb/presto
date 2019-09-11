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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcFileTailSource;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.raptor.filesystem.FileSystemContext;
import com.facebook.presto.raptor.metadata.ShardDeleteDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.LongArrayBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.storage.OrcStorageManager.HUGE_MAX_READ_BLOCK_SIZE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class DeltaShardRewriter
        implements ShardRewriter
{
    private static final JsonCodec<ShardDeleteDelta> SHARD_DELETE_DELTA_CODEC = jsonCodec(ShardDeleteDelta.class);
    private final OrcStorageManager orcStorageManager;
    private final FileSystemContext fileSystemContext;
    private final FileSystem fileSystem;
    private final OrcFileTailSource orcFileTailSource;
    private final long transactionId;
    private final OptionalInt bucketNumber;
    private final UUID oldShardUuid;
    private final Optional<UUID> oldDeltaDeleteShardUuid;
    private final ExecutorService deletionExecutor;
    private final ReaderAttributes defaultReaderAttributes;

    public DeltaShardRewriter(
            FileSystemContext fileSystemContext,
            FileSystem fileSystem,
            OrcFileTailSource orcFileTailSource,
            long transactionId,
            OptionalInt bucketNumber,
            UUID oldShardUuid,
            Optional<UUID> oldDeltaDeleteShardUuid,
            OrcStorageManager orcStorageManager,
            ExecutorService deletionExecutor,
            ReaderAttributes defaultReaderAttributes)
    {
        this.orcStorageManager = orcStorageManager;
        this.fileSystemContext = fileSystemContext;
        this.fileSystem = fileSystem;
        this.orcFileTailSource = orcFileTailSource;
        this.transactionId = transactionId;
        this.bucketNumber = bucketNumber;
        this.oldShardUuid = oldShardUuid;
        this.oldDeltaDeleteShardUuid = oldDeltaDeleteShardUuid;
        this.deletionExecutor = deletionExecutor;
        this.defaultReaderAttributes = defaultReaderAttributes;
    }

    @Override
    public CompletableFuture<Collection<Slice>> rewrite(BitSet rowsToDelete)
    {
        if (rowsToDelete.isEmpty()) {
            return completedFuture(ImmutableList.of());
        }
        return supplyAsync(() -> writeDeltaDeleteFile(rowsToDelete), deletionExecutor);
    }

    @VisibleForTesting
    Collection<Slice> writeDeltaDeleteFile(BitSet rowsToDelete)
    {
        if (rowsToDelete.isEmpty()) {
            return ImmutableList.of();
        }
        // blockToDelete is LongArrayBlock
        List<Long> columnIds = new ArrayList<>();
        List<Type> columnTypes = new ArrayList<>();
        columnIds.add(0L);
        columnTypes.add(BIGINT);

        // TODO: Under current implementation, one block can only hold INT_MAX many rows
        //  which theoretically may not be enough to hold all rows from an ORC file.
        // At this point, rowsToDelete couldn't be empty
        oldDeltaDeleteShardUuid.ifPresent(oldDeltaDeleteShardUuid -> mergeToRowsToDelete(rowsToDelete, oldDeltaDeleteShardUuid));

        if (rowsToDelete.cardinality() == getRowCount(oldShardUuid)) {
            // Delete original file
            return shardDeleteDelta(oldShardUuid, oldDeltaDeleteShardUuid, Optional.empty());
        }

        BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, rowsToDelete.size());
        for (int i = rowsToDelete.nextSetBit(0); i >= 0; i = rowsToDelete.nextSetBit(i + 1)) {
            blockBuilder.writeLong(i);
        }
        Block blockToDelete = blockBuilder.build();
        // TODO: a async call made by deletionExecutor, it calls into OrcPageSink which uses a different thread pool (i.e. commitExecutor)
        //  Better use consistent thread pool management
        StoragePageSink pageSink = orcStorageManager.createStoragePageSink(fileSystemContext, transactionId, bucketNumber, columnIds, columnTypes, true);
        pageSink.appendPages(ImmutableList.of(new Page(blockToDelete)));
        List<ShardInfo> shardInfos = getFutureValue(pageSink.commit());
        // Guaranteed that shardInfos only has one element since we only call commit one time
        ShardInfo newDeltaDeleteShard = Iterables.getOnlyElement(shardInfos);
        return shardDeleteDelta(oldShardUuid, oldDeltaDeleteShardUuid, Optional.of(newDeltaDeleteShard));
    }

    // Note: This function will change rowsToDelete.
    // Will merge the BitSet from oldDeltaDeleteShardUuid to rowsToDelete
    // rowsToDelete and rowsDeleted must be mutually exclusive
    private void mergeToRowsToDelete(BitSet rowsToDelete, UUID oldDeltaDeleteShardUuid)
    {
        Optional<BitSet> rowsDeleted = orcStorageManager.getRowsFromUuid(fileSystem, Optional.of(oldDeltaDeleteShardUuid));
        BitSet verify = new BitSet();
        verify.or(rowsToDelete);
        verify.and(rowsDeleted.get());
        if (verify.cardinality() != 0) {
            throw new PrestoException(RAPTOR_ERROR, "rowsToDelete and rowsDeleted are not mutually exclusive");
        }
        if (rowsDeleted.isPresent()) {
            rowsToDelete.or(rowsDeleted.get());
        }
    }

    private int getRowCount(UUID oldShardUuid)
    {
        try (OrcDataSource dataSource = orcStorageManager.openShard(fileSystem, oldShardUuid, defaultReaderAttributes)) {
            OrcReader reader = new OrcReader(dataSource, ORC, defaultReaderAttributes.getMaxMergeDistance(), defaultReaderAttributes.getTinyStripeThreshold(), HUGE_MAX_READ_BLOCK_SIZE, orcFileTailSource);
            if (reader.getFooter().getNumberOfRows() >= Integer.MAX_VALUE) {
                throw new IOException("File has too many rows");
            }
            return toIntExact(reader.getFooter().getNumberOfRows());
        }
        catch (Exception e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to read file: " + oldShardUuid, e);
        }
    }

    private Collection<Slice> shardDeleteDelta(UUID oldShardUuid, Optional<UUID> oldDeltaDeleteShard, Optional<ShardInfo> newDeltaDeleteShard)
    {
        return ImmutableList.of(Slices.wrappedBuffer(SHARD_DELETE_DELTA_CODEC.toJsonBytes(
                new ShardDeleteDelta(oldShardUuid, oldDeltaDeleteShard, newDeltaDeleteShard))));
    }
}
