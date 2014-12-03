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

import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.storage.StorageService.createParents;
import static com.facebook.presto.raptor.storage.StorageService.temporarySuffix;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ShardRecoveryManager
{
    private static final Logger log = Logger.get(ShardRecoveryManager.class);
    private static final int MAX_THREADS_PER_POOL = 10;

    private enum RecoveryType
    {
        ACTIVE,
        BACKGROUND
    }

    private final StorageService storageService;
    private final String nodeIdentifier;
    private final ShardManager shardManager;

    private final AtomicBoolean enqueueMissingShardsStarted = new AtomicBoolean();
    private final ExecutorService missingShardExecutor = newSingleThreadExecutor(daemonThreadsNamed("missing-shard-identifier"));

    private final ListeningExecutorService backgroundShardRecovery = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(MAX_THREADS_PER_POOL, daemonThreadsNamed("background-shard-recovery")));
    private final ListeningExecutorService activeShardRecovery = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(MAX_THREADS_PER_POOL, daemonThreadsNamed("active-shard-recovery")));

    private final LoadingCache<MissingShard, ListenableFuture<?>> queuedMissingShards = CacheBuilder.newBuilder().build(new CacheLoader<MissingShard, ListenableFuture<?>>()
    {
        @Override
        public ListenableFuture<?> load(final MissingShard missingShard)
                throws Exception
        {
            MissingShardRecovery task = new MissingShardRecovery(missingShard.getShardUuid());
            ListenableFuture<?> future;
            if (missingShard.getRecoveryType() == RecoveryType.ACTIVE) {
                future = activeShardRecovery.submit(task);
            }
            else {
                future = backgroundShardRecovery.submit(task);
            }
            future.addListener(new Runnable()
            {
                @Override
                public void run()
                {
                    queuedMissingShards.invalidate(missingShard);
                }
            }, MoreExecutors.directExecutor());
            return future;
        }
    });

    @Inject
    public ShardRecoveryManager(StorageService storageService, NodeManager nodeManager, ShardManager shardManager)
    {
        this.storageService = checkNotNull(storageService, "storageManagerUtil is null");
        this.nodeIdentifier = checkNotNull(nodeManager, "nodeManager is null").getCurrentNode().getNodeIdentifier();
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
    }

    @PostConstruct
    public void start()
    {
        if (!storageService.isBackupAvailable()) {
            return;
        }
        if (enqueueMissingShardsStarted.compareAndSet(false, true)) {
            enqueueMissingShards();
        }
    }

    public void enqueueMissingShards()
    {
        missingShardExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                ThreadLocalRandom sleepTime = ThreadLocalRandom.current();
                while (true) {
                    try {
                        for (UUID missingShard : getMissingShards()) {
                            queuedMissingShards.get(new MissingShard(missingShard, RecoveryType.BACKGROUND));
                        }
                        SECONDS.sleep(sleepTime.nextInt(1, 30));
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    catch (Throwable t) {
                        log.error(t, "Error recovering shards");
                    }
                }
            }
        });
    }

    private Set<UUID> getMissingShards()
    {
        Set<UUID> shardUuids = shardManager.getNodeShards(nodeIdentifier);
        ImmutableSet.Builder<UUID> missingShards = ImmutableSet.builder();
        for (UUID shardUuid : shardUuids) {
            File storageFile = storageService.getStorageFile(shardUuid).getAbsoluteFile();
            if (!storageFile.exists()) {
                missingShards.add(shardUuid);
            }
        }
        return missingShards.build();
    }

    public Future<?> addActiveMissingShard(UUID shardUuid)
    {
        checkNotNull(shardUuid, "shardUuid is null");
        try {
            return queuedMissingShards.get(new MissingShard(shardUuid, RecoveryType.ACTIVE));
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    void restoreFromBackup(UUID shardUuid)
    {
        File storageFile = storageService.getStorageFile(shardUuid).getAbsoluteFile();
        if (storageFile.exists()) {
            return;
        }

        File backupFile = storageService.getBackupFile(shardUuid);
        if (!backupFile.exists()) {
            log.info("No backup file found for shard: %s", shardUuid);
            return;
        }

        // create a temporary file in the staging directory
        File stagingFile = temporarySuffix(storageService.getStagingFile(shardUuid));
        createParents(stagingFile);

        // copy to temporary file
        long start = System.nanoTime();

        try {
            Files.copy(backupFile.toPath(), stagingFile.toPath());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to copy backup shard file: " + backupFile, e);
        }

        Duration duration = nanosSince(start);
        DataSize size = new DataSize(stagingFile.length(), BYTE);
        DataSize rate = dataRate(size, duration);
        log.info("Copied shard %s from backup in %s (%s at %s/s)", shardUuid, duration, size, rate);

        // move to final location
        createParents(storageFile);
        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (FileAlreadyExistsException e) {
            // someone else already created it (should not happen, but safe to ignore)
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to move shard file", e);
        }
    }

    private class MissingShardRecovery
            implements Callable<Void>
    {
        private final UUID shardUuid;

        public MissingShardRecovery(UUID shardUuid)
        {
            this.shardUuid = shardUuid;
        }

        @Override
        public Void call()
        {
            restoreFromBackup(shardUuid);
            return null;
        }
    }

    private static final class MissingShard
    {
        private final UUID shardUuid;

        public RecoveryType getRecoveryType()
        {
            return recoveryType;
        }

        private final RecoveryType recoveryType;

        public MissingShard(UUID shardUuid, RecoveryType recoveryType)
        {
            this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
            this.recoveryType = checkNotNull(recoveryType, "recoveryType is null");
        }

        public UUID getShardUuid()
        {
            return shardUuid;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MissingShard that = (MissingShard) o;
            return Objects.equal(this.shardUuid, that.shardUuid) &&
                    Objects.equal(this.recoveryType, that.recoveryType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(shardUuid, recoveryType);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("shardUuid", shardUuid)
                    .add("recoveryType", recoveryType)
                    .toString();
        }
    }

    private static DataSize dataRate(DataSize size, Duration duration)
    {
        double rate = size.toBytes() / duration.getValue(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }
        return new DataSize(rate, BYTE).convertToMostSuccinctDataSize();
    }
}
