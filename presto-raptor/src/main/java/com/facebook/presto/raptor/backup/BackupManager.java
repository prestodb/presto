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
package com.facebook.presto.raptor.backup;

import com.facebook.presto.raptor.storage.BackupStats;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;

import java.io.File;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class BackupManager
{
    private final Optional<BackupStore> backupStore;
    private final ExecutorService executorService;

    private final AtomicInteger pendingBackups = new AtomicInteger();
    private final BackupStats stats = new BackupStats();

    @Inject
    public BackupManager(Optional<BackupStore> backupStore, BackupConfig config)
    {
        this(backupStore, config.getBackupThreads());
    }

    public BackupManager(Optional<BackupStore> backupStore, int backupThreads)
    {
        checkArgument(backupThreads > 0, "backupThreads must be > 0");

        this.backupStore = requireNonNull(backupStore, "backupStore is null");
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

        pendingBackups.incrementAndGet();
        return runAsync(new BackgroundBackup(uuid, source), executorService)
                .whenComplete((none, throwable) -> pendingBackups.decrementAndGet());
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
                stats.incrementBackupSuccess();
            }
            catch (Throwable t) {
                stats.incrementBackupFailure();
                throw Throwables.propagate(t);
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
}
