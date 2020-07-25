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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.raptor.metadata.DeltaInfoPair;
import com.facebook.presto.raptor.metadata.ShardDeleteDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FileSystem;

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
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class DeltaShardRewriter
        implements ShardRewriter
{
    private static final JsonCodec<ShardDeleteDelta> SHARD_DELETE_DELTA_CODEC = jsonCodec(ShardDeleteDelta.class);

    private final UUID oldShardUuid;
    private final int oldShardRowCount;
    private final Optional<UUID> oldDeltaDeleteShardUuid;
    private final ExecutorService deletionExecutor;
    private final long transactionId;
    private final OptionalInt bucketNumber;
    private final OrcStorageManager orcStorageManager;
    private final HdfsContext hdfsContext;
    private final FileSystem fileSystem;

    public DeltaShardRewriter(
            UUID oldShardUuid,
            int oldShardRowCount,
            Optional<UUID> oldDeltaDeleteShardUuid,
            ExecutorService deletionExecutor,
            long transactionId,
            OptionalInt bucketNumber,
            OrcStorageManager orcStorageManager,
            HdfsContext hdfsContext,
            FileSystem fileSystem)
    {
        this.oldShardUuid = requireNonNull(oldShardUuid, "oldShardUuid is null");
        this.oldShardRowCount = oldShardRowCount;
        this.oldDeltaDeleteShardUuid = requireNonNull(oldDeltaDeleteShardUuid, "Optional oldDeltaDeleteShardUuid is null");
        this.deletionExecutor = requireNonNull(deletionExecutor, "deletionExecutor is null");
        this.transactionId = transactionId;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.orcStorageManager = requireNonNull(orcStorageManager, "orcStorageManager is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
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

        // TODO: Under current implementation, one block can only hold INT_MAX many rows
        //  which theoretically may not be enough to hold all rows from an ORC file.
        // At this point, rowsToDelete couldn't be empty
        oldDeltaDeleteShardUuid.ifPresent(oldDeltaDeleteShardUuid -> mergeToRowsToDelete(rowsToDelete, oldDeltaDeleteShardUuid));

        if (rowsToDelete.cardinality() == oldShardRowCount) {
            // Delete original file
            return shardDeleteDelta(oldShardUuid, oldDeltaDeleteShardUuid, Optional.empty());
        }

        BlockBuilder blockBuilder = new LongArrayBlockBuilder(null, rowsToDelete.size());
        for (int i = rowsToDelete.nextSetBit(0); i >= 0; i = rowsToDelete.nextSetBit(i + 1)) {
            blockBuilder.writeLong(i);
        }
        // blockToDelete is LongArrayBlock
        StoragePageSink pageSink = orcStorageManager.createStoragePageSink(hdfsContext, transactionId, bucketNumber, ImmutableList.of(0L), ImmutableList.of(BIGINT), true);
        pageSink.appendPages(ImmutableList.of(new Page(blockBuilder.build())));
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
        if (!rowsDeleted.isPresent()) {
            return;
        }

        BitSet verify = new BitSet();
        verify.or(rowsToDelete);
        verify.and(rowsDeleted.get());
        if (verify.cardinality() != 0) {
            throw new PrestoException(RAPTOR_ERROR, "rowsToDelete and rowsDeleted are not mutually exclusive");
        }
        rowsToDelete.or(rowsDeleted.get());
    }

    private static Collection<Slice> shardDeleteDelta(UUID oldShardUuid, Optional<UUID> oldDeltaDeleteShard, Optional<ShardInfo> newDeltaDeleteShard)
    {
        return ImmutableList.of(Slices.wrappedBuffer(SHARD_DELETE_DELTA_CODEC.toJsonBytes(
                new ShardDeleteDelta(oldShardUuid, new DeltaInfoPair(oldDeltaDeleteShard, newDeltaDeleteShard)))));
    }
}
