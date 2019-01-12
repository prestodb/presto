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
package io.prestosql.plugin.raptor.legacy.backup;

import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.raptor.legacy.storage.BackupStats;
import io.prestosql.plugin.raptor.legacy.storage.StorageService;
import io.prestosql.spi.PrestoException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_BACKUP_CORRUPTION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class BackupManager
{
    private static final Logger log = Logger.get(BackupManager.class);

    private final Optional<BackupStore> backupStore;
    private final StorageService storageService;
    private final ExecutorService executorService;

    private final AtomicInteger pendingBackups = new AtomicInteger();
    private final BackupStats stats = new BackupStats();

    @Inject
    public BackupManager(Optional<BackupStore> backupStore, StorageService storageService, BackupConfig config)
    {
        this(backupStore, storageService, config.getBackupThreads());
    }

    public BackupManager(Optional<BackupStore> backupStore, StorageService storageService, int backupThreads)
    {
        checkArgument(backupThreads > 0, "backupThreads must be > 0");

        this.backupStore = requireNonNull(backupStore, "backupStore is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.executorService = newFixedThreadPool(backupThreads, daemonThreadsNamed("background-shard-backup-%s"));
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
    }

    public CompletableFuture<?> submit(UUID uuid, File source)
    {
        requireNonNull(uuid, "uuid is null");
        requireNonNull(source, "source is null");

        if (!backupStore.isPresent()) {
            return completedFuture(null);
        }

        // TODO: decrement when the running task is finished (not immediately on cancel)
        pendingBackups.incrementAndGet();
        CompletableFuture<?> future = runAsync(new BackgroundBackup(uuid, source), executorService);
        future.whenComplete((none, throwable) -> pendingBackups.decrementAndGet());
        return future;
    }

    private class BackgroundBackup
            implements Runnable
    {
        private final UUID uuid;
        private final File source;
        private final long queuedTime = System.nanoTime();

        public BackgroundBackup(UUID uuid, File source)
        {
            this.uuid = requireNonNull(uuid, "uuid is null");
            this.source = requireNonNull(source, "source is null");
        }

        @Override
        public void run()
        {
            try {
                stats.addQueuedTime(Duration.nanosSince(queuedTime));
                long start = System.nanoTime();

                backupStore.get().backupShard(uuid, source);
                stats.addCopyShardDataRate(new DataSize(source.length(), BYTE), Duration.nanosSince(start));

                File restored = new File(storageService.getStagingFile(uuid) + ".validate");
                backupStore.get().restoreShard(uuid, restored);

                if (!filesEqual(source, restored)) {
                    stats.incrementBackupCorruption();

                    File quarantineBase = storageService.getQuarantineFile(uuid);
                    File quarantineOriginal = new File(quarantineBase.getPath() + ".original");
                    File quarantineRestored = new File(quarantineBase.getPath() + ".restored");

                    log.error("Backup is corrupt after write. Quarantining local file: %s", quarantineBase);
                    if (!this.source.renameTo(quarantineOriginal) || !restored.renameTo(quarantineRestored)) {
                        log.warn("Quarantine of corrupt backup shard failed: %s", uuid);
                    }

                    throw new PrestoException(RAPTOR_BACKUP_CORRUPTION, "Backup is corrupt after write: " + uuid);
                }

                if (!restored.delete()) {
                    log.warn("Failed to delete staging file: %s", restored);
                }

                stats.incrementBackupSuccess();
            }
            catch (Throwable t) {
                stats.incrementBackupFailure();
                throw t;
            }
        }
    }

    @Managed
    public int getPendingBackupCount()
    {
        return pendingBackups.get();
    }

    @Managed
    @Flatten
    public BackupStats getStats()
    {
        return stats;
    }

    private static boolean filesEqual(File file1, File file2)
    {
        try {
            return Files.equal(file1, file2);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
