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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.raptor.util.DaoSupplier;
import com.facebook.presto.spi.NodeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptor.metadata.ShardDao.CLEANABLE_SHARDS_BATCH_SIZE;
import static com.facebook.presto.raptor.metadata.ShardDao.CLEANUP_TRANSACTIONS_BATCH_SIZE;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class ShardCleaner
{
    private static final Logger log = Logger.get(ShardCleaner.class);

    private final ShardDao dao;
    private final String currentNode;
    private final boolean coordinator;
    private final Ticker ticker;
    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final Duration maxTransactionAge;
    private final Duration transactionCleanerInterval;
    private final Duration localCleanerInterval;
    private final Duration localCleanTime;
    private final Duration backupCleanerInterval;
    private final Duration backupCleanTime;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService backupExecutor;
    private final Duration maxCompletedTransactionAge;

    private final AtomicBoolean started = new AtomicBoolean();

    private final CounterStat transactionJobErrors = new CounterStat();
    private final CounterStat backupJobErrors = new CounterStat();
    private final CounterStat localJobErrors = new CounterStat();
    private final CounterStat backupShardsQueued = new CounterStat();
    private final CounterStat backupShardsCleaned = new CounterStat();
    private final CounterStat localShardsCleaned = new CounterStat();

    @GuardedBy("this")
    private final Map<UUID, Long> shardsToClean = new HashMap<>();

    @Inject
    public ShardCleaner(
            DaoSupplier<ShardDao> shardDaoSupplier,
            Ticker ticker,
            NodeManager nodeManager,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ShardCleanerConfig config)
    {
        this(
                shardDaoSupplier,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                nodeManager.getCurrentNode().isCoordinator(),
                ticker,
                storageService,
                backupStore,
                config.getMaxTransactionAge(),
                config.getTransactionCleanerInterval(),
                config.getLocalCleanerInterval(),
                config.getLocalCleanTime(),
                config.getBackupCleanerInterval(),
                config.getBackupCleanTime(),
                config.getBackupDeletionThreads(),
                config.getMaxCompletedTransactionAge());
    }

    public ShardCleaner(
            DaoSupplier<ShardDao> shardDaoSupplier,
            String currentNode,
            boolean coordinator,
            Ticker ticker,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            Duration maxTransactionAge,
            Duration transactionCleanerInterval,
            Duration localCleanerInterval,
            Duration localCleanTime,
            Duration backupCleanerInterval,
            Duration backupCleanTime,
            int backupDeletionThreads,
            Duration maxCompletedTransactionAge)
    {
        this.dao = shardDaoSupplier.onDemand();
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.coordinator = coordinator;
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.maxTransactionAge = requireNonNull(maxTransactionAge, "maxTransactionAge");
        this.transactionCleanerInterval = requireNonNull(transactionCleanerInterval, "transactionCleanerInterval is null");
        this.localCleanerInterval = requireNonNull(localCleanerInterval, "localCleanerInterval is null");
        this.localCleanTime = requireNonNull(localCleanTime, "localCleanTime is null");
        this.backupCleanerInterval = requireNonNull(backupCleanerInterval, "backupCleanerInterval is null");
        this.backupCleanTime = requireNonNull(backupCleanTime, "backupCleanTime is null");
        this.scheduler = newScheduledThreadPool(2, daemonThreadsNamed("shard-cleaner-%s"));
        this.backupExecutor = newFixedThreadPool(backupDeletionThreads, daemonThreadsNamed("shard-cleaner-backup-%s"));
        this.maxCompletedTransactionAge = requireNonNull(maxCompletedTransactionAge, "maxCompletedTransactionAge is null");
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            startJobs();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        scheduler.shutdownNow();
        backupExecutor.shutdownNow();
    }

    @Managed
    @Nested
    public CounterStat getTransactionJobErrors()
    {
        return transactionJobErrors;
    }

    @Managed
    @Nested
    public CounterStat getBackupJobErrors()
    {
        return backupJobErrors;
    }

    @Managed
    @Nested
    public CounterStat getLocalJobErrors()
    {
        return localJobErrors;
    }

    @Managed
    @Nested
    public CounterStat getBackupShardsQueued()
    {
        return backupShardsQueued;
    }

    @Managed
    @Nested
    public CounterStat getBackupShardsCleaned()
    {
        return backupShardsCleaned;
    }

    @Managed
    @Nested
    public CounterStat getLocalShardsCleaned()
    {
        return localShardsCleaned;
    }

    private void startJobs()
    {
        if (coordinator) {
            startTransactionCleanup();
            if (backupStore.isPresent()) {
                startBackupCleanup();
            }
        }

        // We can only delete local shards if the backup store is present,
        // since there is a race condition between shards getting created
        // on a worker and being committed (referenced) in the database.
        if (backupStore.isPresent()) {
            startLocalCleanup();
        }
    }

    private void startTransactionCleanup()
    {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                abortOldTransactions();
                deleteOldCompletedTransactions();
                deleteOldShards();
            }
            catch (Throwable t) {
                log.error(t, "Error cleaning transactions");
                transactionJobErrors.update(1);
            }
        }, 0, transactionCleanerInterval.toMillis(), MILLISECONDS);
    }

    private void startBackupCleanup()
    {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                cleanBackupShards();
            }
            catch (Throwable t) {
                log.error(t, "Error cleaning backup shards");
                backupJobErrors.update(1);
            }
        }, 0, backupCleanerInterval.toMillis(), MILLISECONDS);
    }

    private void startLocalCleanup()
    {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                // jitter to avoid overloading database
                long interval = this.localCleanerInterval.roundTo(SECONDS);
                SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, interval));
                cleanLocalShards();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error cleaning local shards");
                localJobErrors.update(1);
            }
        }, 0, localCleanerInterval.toMillis(), MILLISECONDS);
    }

    @VisibleForTesting
    void abortOldTransactions()
    {
        dao.abortOldTransactions(maxTimestamp(maxTransactionAge));
    }

    @VisibleForTesting
    void deleteOldShards()
    {
        while (!Thread.currentThread().isInterrupted()) {
            List<UUID> shards = dao.getOldCreatedShardsBatch();
            if (shards.isEmpty()) {
                break;
            }
            dao.insertDeletedShards(shards);
            dao.deleteCreatedShards(shards);
            backupShardsQueued.update(shards.size());
        }
    }

    @VisibleForTesting
    void deleteOldCompletedTransactions()
    {
        while (!Thread.currentThread().isInterrupted()) {
            int deleted = dao.deleteOldCompletedTransactions(maxTimestamp(maxCompletedTransactionAge));
            if (deleted < CLEANUP_TRANSACTIONS_BATCH_SIZE) {
                break;
            }
        }
    }

    @VisibleForTesting
    synchronized void cleanLocalShards()
    {
        // find all files on the local node
        Set<UUID> local = storageService.getStorageShards();

        // get shards assigned to the local node
        Set<UUID> assigned = dao.getNodeShards(currentNode, null).stream()
                .map(ShardMetadata::getShardUuid)
                .collect(toSet());

        // un-mark previously marked files that are now assigned
        for (UUID uuid : assigned) {
            shardsToClean.remove(uuid);
        }

        // mark all files that are not assigned
        for (UUID uuid : local) {
            if (!assigned.contains(uuid)) {
                shardsToClean.putIfAbsent(uuid, ticker.read());
            }
        }

        // delete files marked earlier than the clean interval
        long threshold = ticker.read() - localCleanTime.roundTo(NANOSECONDS);
        Set<UUID> deletions = shardsToClean.entrySet().stream()
                .filter(entry -> entry.getValue() < threshold)
                .map(Map.Entry::getKey)
                .collect(toSet());
        if (deletions.isEmpty()) {
            return;
        }

        for (UUID uuid : deletions) {
            deleteFile(storageService.getStorageFile(uuid));
            shardsToClean.remove(uuid);
        }

        localShardsCleaned.update(deletions.size());
        log.info("Cleaned %s local shards", deletions.size());
    }

    @VisibleForTesting
    void cleanBackupShards()
    {
        Set<UUID> processing = newConcurrentHashSet();
        BlockingQueue<UUID> completed = new LinkedBlockingQueue<>();
        boolean fill = true;

        while (!Thread.currentThread().isInterrupted()) {
            // get a new batch if any completed and we are under the batch size
            Set<UUID> uuids = ImmutableSet.of();
            if (fill && (processing.size() < CLEANABLE_SHARDS_BATCH_SIZE)) {
                uuids = dao.getCleanableShardsBatch(maxTimestamp(backupCleanTime));
                fill = false;
            }
            if (uuids.isEmpty() && processing.isEmpty()) {
                break;
            }

            // skip any that are already processing and mark remaining as processing
            uuids = ImmutableSet.copyOf(difference(uuids, processing));
            processing.addAll(uuids);

            // execute deletes
            for (UUID uuid : uuids) {
                runAsync(() -> backupStore.get().deleteShard(uuid), backupExecutor)
                        .thenAccept(v -> completed.add(uuid))
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                log.error(e, "Error cleaning backup shard: %s", uuid);
                                backupJobErrors.update(1);
                                processing.remove(uuid);
                            }
                        });
            }

            // get the next batch of completed deletes
            int desired = min(100, processing.size());
            Collection<UUID> done = drain(completed, desired, 100, MILLISECONDS);
            if (done.isEmpty()) {
                continue;
            }

            // remove completed deletes from database
            processing.removeAll(done);
            dao.deleteCleanedShards(done);
            backupShardsCleaned.update(done.size());
            fill = true;
        }
    }

    private static <T> Collection<T> drain(BlockingQueue<T> queue, int desired, long timeout, TimeUnit unit)
    {
        long start = System.nanoTime();
        Collection<T> result = new ArrayList<>();

        while (true) {
            queue.drainTo(result);
            if (result.size() >= desired) {
                return result;
            }

            long elapsedNanos = System.nanoTime() - start;
            long remainingNanos = unit.toNanos(timeout) - elapsedNanos;
            if (remainingNanos <= 0) {
                return result;
            }

            try {
                result.add(queue.poll(remainingNanos, NANOSECONDS));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return result;
            }
        }
    }

    private static Timestamp maxTimestamp(Duration duration)
    {
        return new Timestamp(System.currentTimeMillis() - duration.toMillis());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void deleteFile(File file)
    {
        file.delete();
    }
}
