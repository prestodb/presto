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
package io.prestosql.plugin.raptor.legacy.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.raptor.legacy.backup.BackupStore;
import io.prestosql.plugin.raptor.legacy.metadata.ShardManager;
import io.prestosql.plugin.raptor.legacy.metadata.ShardMetadata;
import io.prestosql.plugin.raptor.legacy.util.PrioritizedFifoExecutor;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.DataSize.succinctDataSize;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_BACKUP_CORRUPTION;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_ERROR;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static io.prestosql.plugin.raptor.legacy.storage.OrcStorageManager.xxhash64;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class ShardRecoveryManager
{
    private static final Logger log = Logger.get(ShardRecoveryManager.class);

    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final String nodeIdentifier;
    private final ShardManager shardManager;
    private final Duration missingShardDiscoveryInterval;

    private final AtomicBoolean started = new AtomicBoolean();
    private final MissingShardsQueue shardQueue;

    private final ScheduledExecutorService missingShardExecutor = newScheduledThreadPool(1, daemonThreadsNamed("missing-shard-discovery"));
    private final ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("shard-recovery-%s"));
    private final ShardRecoveryStats stats;

    @Inject
    public ShardRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            NodeManager nodeManager,
            ShardManager shardManager,
            StorageManagerConfig config)
    {
        this(storageService,
                backupStore,
                nodeManager,
                shardManager,
                config.getMissingShardDiscoveryInterval(),
                config.getRecoveryThreads());
    }

    public ShardRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            NodeManager nodeManager,
            ShardManager shardManager,
            Duration missingShardDiscoveryInterval,
            int recoveryThreads)
    {
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.nodeIdentifier = requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getNodeIdentifier();
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.missingShardDiscoveryInterval = requireNonNull(missingShardDiscoveryInterval, "missingShardDiscoveryInterval is null");
        this.shardQueue = new MissingShardsQueue(new PrioritizedFifoExecutor<>(executorService, recoveryThreads, new MissingShardComparator()));
        this.stats = new ShardRecoveryStats();
    }

    @PostConstruct
    public void start()
    {
        if (!backupStore.isPresent()) {
            return;
        }
        if (started.compareAndSet(false, true)) {
            scheduleRecoverMissingShards();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
        missingShardExecutor.shutdownNow();
    }

    private void scheduleRecoverMissingShards()
    {
        missingShardExecutor.scheduleWithFixedDelay(() -> {
            try {
                // jitter to avoid overloading database
                long interval = missingShardDiscoveryInterval.roundTo(SECONDS);
                SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, interval));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            enqueueMissingShards();
        }, 0, missingShardDiscoveryInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Managed
    public void recoverMissingShards()
    {
        missingShardExecutor.submit(this::enqueueMissingShards);
    }

    private synchronized void enqueueMissingShards()
    {
        try {
            for (ShardMetadata shard : getMissingShards()) {
                stats.incrementBackgroundShardRecovery();
                ListenableFuture<?> future = shardQueue.submit(new MissingShard(shard.getShardUuid(), shard.getCompressedSize(), shard.getXxhash64(), false));
                addExceptionCallback(future, t -> log.warn(t, "Error recovering shard: %s", shard.getShardUuid()));
            }
        }
        catch (Throwable t) {
            log.error(t, "Error creating shard recovery tasks");
        }
    }

    private Set<ShardMetadata> getMissingShards()
    {
        return shardManager.getNodeShards(nodeIdentifier).stream()
                .filter(shard -> shardNeedsRecovery(shard.getShardUuid(), shard.getCompressedSize()))
                .collect(toSet());
    }

    private boolean shardNeedsRecovery(UUID shardUuid, long shardSize)
    {
        File storageFile = storageService.getStorageFile(shardUuid);
        return !storageFile.exists() || (storageFile.length() != shardSize);
    }

    public Future<?> recoverShard(UUID shardUuid)
            throws ExecutionException
    {
        ShardMetadata shard = shardManager.getShard(shardUuid);
        if (shard == null) {
            throw new PrestoException(RAPTOR_ERROR, "Shard does not exist in database: " + shardUuid);
        }
        stats.incrementActiveShardRecovery();
        return shardQueue.submit(new MissingShard(shardUuid, shard.getCompressedSize(), shard.getXxhash64(), true));
    }

    @VisibleForTesting
    void restoreFromBackup(UUID shardUuid, long shardSize, OptionalLong shardXxhash64)
    {
        File storageFile = storageService.getStorageFile(shardUuid);

        if (!backupStore.get().shardExists(shardUuid)) {
            stats.incrementShardRecoveryBackupNotFound();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "No backup file found for shard: " + shardUuid);
        }

        if (storageFile.exists()) {
            if (!isFileCorrupt(storageFile, shardSize, shardXxhash64)) {
                return;
            }
            stats.incrementCorruptLocalFile();
            quarantineFile(shardUuid, storageFile, "Local file is corrupt.");
        }

        // create a temporary file in the staging directory
        File stagingFile = temporarySuffix(storageService.getStagingFile(shardUuid));
        storageService.createParents(stagingFile);

        // copy to temporary file
        log.info("Copying shard %s from backup...", shardUuid);
        long start = System.nanoTime();

        try {
            backupStore.get().restoreShard(shardUuid, stagingFile);
        }
        catch (PrestoException e) {
            stats.incrementShardRecoveryFailure();
            stagingFile.delete();
            throw e;
        }

        Duration duration = nanosSince(start);
        DataSize size = succinctBytes(stagingFile.length());
        DataSize rate = dataRate(size, duration).convertToMostSuccinctDataSize();
        stats.addShardRecoveryDataRate(rate, size, duration);

        log.info("Copied shard %s from backup in %s (%s at %s/s)", shardUuid, duration, size, rate);

        // move to final location
        storageService.createParents(storageFile);
        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (FileAlreadyExistsException e) {
            // someone else already created it (should not happen, but safe to ignore)
        }
        catch (IOException e) {
            stats.incrementShardRecoveryFailure();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Failed to move shard: " + shardUuid, e);
        }
        finally {
            stagingFile.delete();
        }

        if (!storageFile.exists()) {
            stats.incrementShardRecoveryFailure();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "File does not exist after recovery: " + shardUuid);
        }

        if (isFileCorrupt(storageFile, shardSize, shardXxhash64)) {
            stats.incrementShardRecoveryFailure();
            stats.incrementCorruptRecoveredFile();
            quarantineFile(shardUuid, storageFile, "Local file is corrupt after recovery.");
            throw new PrestoException(RAPTOR_BACKUP_CORRUPTION, "Backup is corrupt after read: " + shardUuid);
        }

        stats.incrementShardRecoverySuccess();
    }

    private void quarantineFile(UUID shardUuid, File file, String message)
    {
        File quarantine = new File(storageService.getQuarantineFile(shardUuid).getPath() + ".corrupt");
        if (quarantine.exists()) {
            log.warn("%s Quarantine already exists: %s", message, quarantine);
            return;
        }

        log.error("%s Quarantining corrupt file: %s", message, quarantine);
        try {
            Files.move(file.toPath(), quarantine.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            log.warn(e, "Quarantine of corrupt file failed: " + quarantine);
            file.delete();
        }
    }

    private static boolean isFileCorrupt(File file, long size, OptionalLong xxhash64)
    {
        return (file.length() != size) || (xxhash64.isPresent() && (xxhash64(file) != xxhash64.getAsLong()));
    }

    @VisibleForTesting
    static class MissingShardComparator
            implements Comparator<MissingShardRunnable>
    {
        @Override
        public int compare(MissingShardRunnable shard1, MissingShardRunnable shard2)
        {
            if (shard1.isActive() == shard2.isActive()) {
                return 0;
            }
            return shard1.isActive() ? -1 : 1;
        }
    }

    interface MissingShardRunnable
            extends Runnable
    {
        boolean isActive();
    }

    private class MissingShardRecovery
            implements MissingShardRunnable
    {
        private final UUID shardUuid;
        private final long shardSize;
        private final OptionalLong shardXxhash64;
        private final boolean active;

        public MissingShardRecovery(UUID shardUuid, long shardSize, OptionalLong shardXxhash64, boolean active)
        {
            this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
            this.shardSize = shardSize;
            this.shardXxhash64 = requireNonNull(shardXxhash64, "shardXxhash64 is null");
            this.active = active;
        }

        @Override
        public void run()
        {
            restoreFromBackup(shardUuid, shardSize, shardXxhash64);
        }

        @Override
        public boolean isActive()
        {
            return active;
        }
    }

    private static final class MissingShard
    {
        private final UUID shardUuid;
        private final long shardSize;
        private final OptionalLong shardXxhash64;
        private final boolean active;

        public MissingShard(UUID shardUuid, long shardSize, OptionalLong shardXxhash64, boolean active)
        {
            this.shardUuid = requireNonNull(shardUuid, "shardUuid is null");
            this.shardSize = shardSize;
            this.shardXxhash64 = requireNonNull(shardXxhash64, "shardXxhash64 is null");
            this.active = active;
        }

        public UUID getShardUuid()
        {
            return shardUuid;
        }

        public long getShardSize()
        {
            return shardSize;
        }

        public OptionalLong getShardXxhash64()
        {
            return shardXxhash64;
        }

        public boolean isActive()
        {
            return active;
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

            MissingShard other = (MissingShard) o;
            return Objects.equals(this.active, other.active) &&
                    Objects.equals(this.shardUuid, other.shardUuid);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(shardUuid, active);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("shardUuid", shardUuid)
                    .add("active", active)
                    .toString();
        }
    }

    private class MissingShardsQueue
    {
        private final LoadingCache<MissingShard, ListenableFuture<?>> queuedMissingShards;

        public MissingShardsQueue(PrioritizedFifoExecutor<MissingShardRunnable> shardRecoveryExecutor)
        {
            requireNonNull(shardRecoveryExecutor, "shardRecoveryExecutor is null");
            this.queuedMissingShards = CacheBuilder.newBuilder().build(new CacheLoader<MissingShard, ListenableFuture<?>>()
            {
                @Override
                public ListenableFuture<?> load(MissingShard missingShard)
                {
                    MissingShardRecovery task = new MissingShardRecovery(
                            missingShard.getShardUuid(),
                            missingShard.getShardSize(),
                            missingShard.getShardXxhash64(),
                            missingShard.isActive());
                    ListenableFuture<?> future = shardRecoveryExecutor.submit(task);
                    future.addListener(() -> queuedMissingShards.invalidate(missingShard), directExecutor());
                    return future;
                }
            });
        }

        public ListenableFuture<?> submit(MissingShard shard)
                throws ExecutionException
        {
            return queuedMissingShards.get(shard);
        }
    }

    static DataSize dataRate(DataSize size, Duration duration)
    {
        double rate = size.toBytes() / duration.getValue(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }
        return succinctDataSize(rate, BYTE);
    }

    private static File temporarySuffix(File file)
    {
        return new File(file.getPath() + ".tmp-" + UUID.randomUUID());
    }

    @Managed
    @Flatten
    public ShardRecoveryStats getStats()
    {
        return stats;
    }

    private static <T> FutureCallback<T> failureCallback(Consumer<Throwable> callback)
    {
        return new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T result) {}

            @Override
            public void onFailure(Throwable throwable)
            {
                callback.accept(throwable);
            }
        };
    }
}
