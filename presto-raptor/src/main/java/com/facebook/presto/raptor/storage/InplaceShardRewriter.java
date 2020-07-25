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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.raptor.backup.BackupManager;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardRecorder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class InplaceShardRewriter
        implements ShardRewriter
{
    private static final JsonCodec<ShardDelta> SHARD_DELTA_CODEC = jsonCodec(ShardDelta.class);

    private final UUID shardUuid;
    private final Map<String, Type> columns;
    private final ExecutorService deletionExecutor;
    private final long transactionId;
    private final OptionalInt bucketNumber;
    private final String nodeId;
    private final OrcStorageManager orcStorageManager;
    private final FileSystem fileSystem;
    private final StorageService storageService;
    private final ShardRecorder shardRecorder;
    private final BackupManager backupManager;

    public InplaceShardRewriter(
            UUID shardUuid,
            Map<String, Type> columns,
            ExecutorService deletionExecutor,
            long transactionId,
            OptionalInt bucketNumber,
            String nodeId,
            OrcStorageManager orcStorageManager,
            FileSystem fileSystem,
            StorageService storageService,
            ShardRecorder shardRecorder,
            BackupManager backupManager)
    {
        this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.deletionExecutor = requireNonNull(deletionExecutor, "deletionExecutor is null");
        this.transactionId = transactionId;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.orcStorageManager = requireNonNull(orcStorageManager, "orcStorageManager is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.shardRecorder = requireNonNull(shardRecorder, "shardRecorder is null");
        this.backupManager = requireNonNull(backupManager, "backupManager is null");
    }

    @Override
    public CompletableFuture<Collection<Slice>> rewrite(BitSet rowsToDelete)
    {
        if (rowsToDelete.isEmpty()) {
            return completedFuture(ImmutableList.of());
        }
        return supplyAsync(() -> rewriteShard(rowsToDelete), deletionExecutor);
    }

    @VisibleForTesting
    Collection<Slice> rewriteShard(BitSet rowsToDelete)
    {
        if (rowsToDelete.isEmpty()) {
            return ImmutableList.of();
        }

        UUID newShardUuid = UUID.randomUUID();
        Path input = storageService.getStorageFile(shardUuid);
        Path output = storageService.getStagingFile(newShardUuid);

        OrcFileInfo info = orcStorageManager.rewriteFile(fileSystem, columns, input, output, rowsToDelete);
        long rowCount = info.getRowCount();

        if (rowCount == 0) {
            return shardDelta(shardUuid, Optional.empty());
        }

        shardRecorder.recordCreatedShard(transactionId, newShardUuid);

        // submit for backup and wait until it finishes
        getFutureValue(backupManager.submit(newShardUuid, output));

        Set<String> nodes = ImmutableSet.of(nodeId);
        long uncompressedSize = info.getUncompressedSize();

        ShardInfo shard = orcStorageManager.createShardInfo(fileSystem, newShardUuid, bucketNumber, output, nodes, rowCount, uncompressedSize);

        orcStorageManager.writeShard(newShardUuid);

        return shardDelta(shardUuid, Optional.of(shard));
    }

    private static Collection<Slice> shardDelta(UUID oldShardUuid, Optional<ShardInfo> shardInfo)
    {
        List<ShardInfo> newShards = shardInfo.map(ImmutableList::of).orElse(ImmutableList.of());
        ShardDelta delta = new ShardDelta(ImmutableList.of(oldShardUuid), newShards);
        return ImmutableList.of(Slices.wrappedBuffer(SHARD_DELTA_CODEC.toJsonBytes(delta)));
    }
}
