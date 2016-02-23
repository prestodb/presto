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
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.callable;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ShardCleaner
{
    private static final Logger log = Logger.get(ShardCleaner.class);

    private final IDBI dbi;
    private final DaoSupplier<ShardDao> shardDaoSupplier;
    private final ShardDao dao;
    private final String currentNode;
    private final boolean coordinator;
    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final Duration maxTransactionAge;
    private final Duration transactionCleanerInterval;
    private final Duration localCleanerInterval;
    private final Duration localCleanTime;
    private final Duration localPurgeTime;
    private final Duration backupCleanerInterval;
    private final Duration backupCleanTime;
    private final Duration backupPurgeTime;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService backupExecutor;

    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public ShardCleaner(
            @ForMetadata IDBI dbi,
            DaoSupplier<ShardDao> shardDaoSupplier,
            NodeManager nodeManager,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ShardCleanerConfig config)
    {
        this(dbi,
                shardDaoSupplier,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                nodeManager.getCoordinators().contains(nodeManager.getCurrentNode()),
                storageService,
                backupStore,
                config.getMaxTransactionAge(),
                config.getTransactionCleanerInterval(),
                config.getLocalCleanerInterval(),
                config.getLocalCleanTime(),
                config.getLocalPurgeTime(),
                config.getBackupCleanerInterval(),
                config.getBackupCleanTime(),
                config.getBackupPurgeTime(),
                config.getBackupDeletionThreads());
    }

    public ShardCleaner(
            IDBI dbi,
            DaoSupplier<ShardDao> shardDaoSupplier,
            String currentNode,
            boolean coordinator,
            StorageService storageService,
            Optional<BackupStore> backupStore,
            Duration maxTransactionAge,
            Duration transactionCleanerInterval,
            Duration localCleanerInterval,
            Duration localCleanTime,
            Duration localPurgeTime,
            Duration backupCleanerInterval,
            Duration backupCleanTime,
            Duration backupPurgeTime,
            int backupDeletionThreads)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.shardDaoSupplier = requireNonNull(shardDaoSupplier, "shardDaoSupplier is null");
        this.dao = shardDaoSupplier.onDemand();
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.coordinator = coordinator;
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.maxTransactionAge = requireNonNull(maxTransactionAge, "maxTransactionAge");
        this.transactionCleanerInterval = requireNonNull(transactionCleanerInterval, "transactionCleanerInterval is null");
        this.localCleanerInterval = requireNonNull(localCleanerInterval, "localCleanerInterval is null");
        this.localCleanTime = requireNonNull(localCleanTime, "localCleanTime is null");
        this.localPurgeTime = requireNonNull(localPurgeTime, "localPurgeTime is null");
        this.backupCleanerInterval = requireNonNull(backupCleanerInterval, "backupCleanerInterval is null");
        this.backupCleanTime = requireNonNull(backupCleanTime, "backupCleanTime is null");
        this.backupPurgeTime = requireNonNull(backupPurgeTime, "backupPurgeTime is null");
        this.scheduler = newScheduledThreadPool(2, daemonThreadsNamed("shard-cleaner-%s"));
        this.backupExecutor = newFixedThreadPool(backupDeletionThreads, daemonThreadsNamed("shard-cleaner-backup-%s"));
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

    private void startJobs()
    {
        if (coordinator) {
            startTransactionCleanup();
            if (backupStore.isPresent()) {
                startBackupCleanup();
            }
        }
        startLocalCleanup();
    }

    private void startTransactionCleanup()
    {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                abortOldTransactions();
                deleteOldShards();
            }
            catch (Throwable t) {
                log.error(t, "Error cleaning transactions");
            }
        }, 0, transactionCleanerInterval.toMillis(), MILLISECONDS);
    }

    private void startBackupCleanup()
    {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                cleanBackupShards();
                purgeBackupShards();
            }
            catch (Throwable t) {
                log.error(t, "Error cleaning backup shards");
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
                purgeLocalShards();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable t) {
                log.error(t, "Error cleaning local shards");
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
        try (Handle handle = dbi.open()) {
            ShardDao dao = shardDaoSupplier.attach(handle);

            dao.dropTableTemporaryCreatedShards();
            dao.createTableTemporaryCreatedShards();
            dao.insertTemporaryCreatedShards();
            dao.insertDeletedShardsFromCreated();
            dao.deleteOldCreatedShards();
            dao.dropTableTemporaryCreatedShards();

            dao.dropTableTemporaryCreatedShardNodes();
            dao.createTableTemporaryCreatedShardNodes();
            dao.insertTemporaryCreatedShardNodes();
            dao.insertDeletedShardNodesFromCreated();
            dao.deleteOldCreatedShardNodes();
            dao.dropTableTemporaryCreatedShardNodes();
        }
    }

    @VisibleForTesting
    void cleanLocalShards()
    {
        while (!Thread.currentThread().isInterrupted()) {
            List<UUID> uuids = dao.getCleanableShardNodesBatch(currentNode, maxTimestamp(localCleanTime));
            if (uuids.isEmpty()) {
                break;
            }

            for (UUID uuid : uuids) {
                deleteFile(storageService.getStorageFile(uuid));
            }
            dao.updateCleanedShardNodes(uuids, getCurrentNodeId());
        }
    }

    @VisibleForTesting
    void purgeLocalShards()
    {
        while (!Thread.currentThread().isInterrupted()) {
            List<UUID> uuids = dao.getPurgableShardNodesBatch(currentNode, maxTimestamp(localPurgeTime));
            if (uuids.isEmpty()) {
                break;
            }

            for (UUID uuid : uuids) {
                deleteFile(storageService.getStorageFile(uuid));
            }
            dao.updatePurgedShardNodes(uuids, getCurrentNodeId());
        }
    }

    @VisibleForTesting
    void cleanBackupShards()
    {
        while (!Thread.currentThread().isInterrupted()) {
            List<UUID> uuids = dao.getCleanableShardsBatch(maxTimestamp(backupCleanTime));
            if (uuids.isEmpty()) {
                break;
            }

            executeDeletes(uuids, (uuid) -> {
                backupStore.get().deleteShard(uuid);
                dao.updateCleanedShard(uuid);
            });
        }
    }

    @VisibleForTesting
    void purgeBackupShards()
    {
        while (!Thread.currentThread().isInterrupted()) {
            List<UUID> uuids = dao.getPurgableShardsBatch(maxTimestamp(backupPurgeTime));
            if (uuids.isEmpty()) {
                break;
            }

            executeDeletes(uuids, (uuid) -> {
                backupStore.get().deleteShard(uuid);
                dao.updatePurgedShard(uuid);
            });
        }
    }

    private void executeDeletes(List<UUID> uuids, Consumer<UUID> consumer)
    {
        List<Callable<Object>> tasks = new ArrayList<>();
        for (UUID uuid : uuids) {
            tasks.add(callable(() -> {
                consumer.accept(uuid);
            }));
        }

        List<Future<Object>> futures;
        try {
            futures = backupExecutor.invokeAll(tasks);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        boolean logged = false;
        for (Future<?> future : futures) {
            try {
                future.get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            catch (ExecutionException e) {
                if (!logged) {
                    logged = true;
                    log.error(e.getCause(), "Error deleting backup shard");
                }
            }
        }
    }

    private int getCurrentNodeId()
    {
        Integer nodeId = dao.getNodeId(currentNode);
        if (nodeId == null) {
            throw new PrestoException(RAPTOR_ERROR, "Node does not exist: " + currentNode);
        }
        return nodeId;
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
