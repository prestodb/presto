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

import com.facebook.presto.raptorx.chunkstore.ChunkStore;
import com.facebook.presto.raptorx.metadata.ChunkFile;
import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.raptorx.util.PrioritizedFifoExecutor;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_CHUNK_STORE_CORRUPTION;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_STORAGE_ERROR;
import static com.facebook.presto.raptorx.util.DataRate.dataRate;
import static com.facebook.presto.raptorx.util.StorageUtil.xxhash64;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class ChunkRecoveryManager
{
    private static final Logger log = Logger.get(ChunkRecoveryManager.class);

    private final StorageService storageService;
    private final ChunkStore chunkStore;
    private final ChunkManager chunkManager;
    private final String nodeIdentifier;
    private final Duration missingChunkDiscoveryInterval;

    private final AtomicBoolean started = new AtomicBoolean();
    private final MissingChunksQueue chunkQueue;

    private final ScheduledExecutorService missingChunkExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("missing-chunk-discovery"));
    private final ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("chunk-recovery-%s"));
    private final ChunkRecoveryStats stats;

    @Inject
    public ChunkRecoveryManager(
            StorageService storageService,
            ChunkStore chunkStore,
            ChunkManager chunkManager,
            NodeManager nodeManager,
            ChunkRecoveryConfig config)
    {
        this(storageService,
                chunkStore,
                chunkManager,
                nodeManager.getCurrentNode().getNodeIdentifier(),
                config.getMissingChunkDiscoveryInterval(),
                config.getRecoveryThreads());
    }

    public ChunkRecoveryManager(
            StorageService storageService,
            ChunkStore chunkStore,
            ChunkManager chunkManager,
            String nodeIdentifier,
            Duration missingChunkDiscoveryInterval,
            int recoveryThreads)
    {
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.chunkStore = requireNonNull(chunkStore, "chunkStore is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null");
        this.missingChunkDiscoveryInterval = requireNonNull(missingChunkDiscoveryInterval, "missingChunkDiscoveryInterval is null");
        this.chunkQueue = new MissingChunksQueue(new PrioritizedFifoExecutor<>(executorService, recoveryThreads, RecoveryRunnable.comparator()));
        this.stats = new ChunkRecoveryStats();
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            scheduleRecoverMissingChunks();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
        missingChunkExecutor.shutdownNow();
    }

    @Managed
    @Flatten
    public ChunkRecoveryStats getStats()
    {
        return stats;
    }

    @Managed
    public void recoverMissingChunks()
    {
        missingChunkExecutor.submit(this::enqueueMissingChunks);
    }

    private void scheduleRecoverMissingChunks()
    {
        missingChunkExecutor.scheduleWithFixedDelay(() -> {
            try {
                // jitter to avoid overloading database
                long interval = missingChunkDiscoveryInterval.roundTo(SECONDS);
                SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, interval));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            enqueueMissingChunks();
        }, 0, missingChunkDiscoveryInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private synchronized void enqueueMissingChunks()
    {
        try {
            for (ChunkFile chunk : getMissingChunks()) {
                stats.incrementBackgroundChunkRecovery();
                addExceptionCallback(
                        chunkQueue.submit(new MissingChunk(false, chunk.getChunkId(), chunk.getSize(), chunk.getXxhash64())),
                        t -> log.warn(t, "Error recovering chunk: %s", chunk.getChunkId()));
            }
        }
        catch (Throwable t) {
            log.error(t, "Error creating chunk recovery tasks");
        }
    }

    private Set<ChunkFile> getMissingChunks()
    {
        return chunkManager.getNodeChunks(nodeIdentifier).stream()
                .filter(chunk -> chunkNeedsRecovery(chunk.getChunkId(), chunk.getSize()))
                .collect(toSet());
    }

    private boolean chunkNeedsRecovery(long chunkId, long chunkSize)
    {
        File storageFile = storageService.getStorageFile(chunkId);
        return !storageFile.exists() || (storageFile.length() != chunkSize);
    }

    public ListenableFuture<?> recoverChunk(long tableId, long chunkId)
    {
        ChunkFile chunk = chunkManager.getChunk(tableId, chunkId);
        stats.incrementActiveChunkRecovery();
        return chunkQueue.submit(new MissingChunk(true, chunk.getChunkId(), chunk.getSize(), chunk.getXxhash64()));
    }

    private void restoreFromChunkStore(long chunkId, long chunkSize, long chunkXxhash64)
    {
        File storageFile = storageService.getStorageFile(chunkId);

        // check if existing file is valid
        if (storageFile.exists()) {
            if (!isFileCorrupt(storageFile, chunkSize, chunkXxhash64)) {
                return;
            }
            stats.incrementCorruptLocalFile();
            quarantineFile(chunkId, storageFile, "Local file is corrupt.");
            deleteFile(storageFile.toPath());
        }

        // fail early if missing in chunk store
        if (!chunkStore.chunkExists(chunkId)) {
            stats.incrementChunkRecoveryNotFound();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Chunk not found in chunk store: " + chunkId);
        }

        // copy to temporary file in the staging directory
        File stagingFile = temporarySuffix(storageService.getStagingFile(chunkId));

        log.info("Copying chunk %s from chunk store...", chunkId);
        long start = System.nanoTime();

        try {
            chunkStore.getChunk(chunkId, stagingFile);
        }
        catch (PrestoException e) {
            stats.incrementChunkRecoveryFailure();
            tryDeleteFile(stagingFile);
            throw e;
        }

        // record transfer rate
        Duration duration = nanosSince(start);
        DataSize size = succinctBytes(stagingFile.length());
        DataSize rate = dataRate(size, duration);
        stats.addChunkRecoveryDataRate(rate, size, duration);

        log.info("Copied chunk %s from chunk store in %s (%s at %s/s)", chunkId, duration, size, rate);

        // check if copied file is valid
        if (isFileCorrupt(stagingFile, chunkSize, chunkXxhash64)) {
            stats.incrementChunkRecoveryFailure();
            stats.incrementCorruptRecoveredFile();
            quarantineFile(chunkId, stagingFile, "File is corrupt after recovery.");
            throw new PrestoException(RAPTOR_CHUNK_STORE_CORRUPTION, "Chunk is corrupt in chunk store: " + chunkId);
        }

        // move to final location
        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (FileAlreadyExistsException e) {
            // someone else already created it (should not happen, but safe to ignore)
        }
        catch (IOException e) {
            stats.incrementChunkRecoveryFailure();
            throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Failed to move chunk: " + chunkId, e);
        }
        finally {
            tryDeleteFile(stagingFile);
        }

        stats.incrementChunkRecoverySuccess();
    }

    private void quarantineFile(long chunkId, File file, String message)
    {
        File quarantine = storageService.getQuarantineFile(chunkId);
        if (quarantine.exists()) {
            log.error("%s Quarantine already exists: %s", message, quarantine);
            return;
        }

        log.error("%s Quarantining corrupt file: %s", message, quarantine);
        try {
            Files.move(file.toPath(), quarantine.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            log.warn(e, "Quarantine of corrupt file failed");
            tryDeleteFile(file);
        }
    }

    private static void deleteFile(Path file)
    {
        try {
            Files.deleteIfExists(file);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to delete local file: " + file, e);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void tryDeleteFile(File file)
    {
        file.delete();
    }

    private static File temporarySuffix(File file)
    {
        return new File(file.getPath() + ".tmp-" + randomUUID());
    }

    private static boolean isFileCorrupt(File file, long size, long xxhash64)
    {
        return (file.length() != size) || (xxhash64(file) != xxhash64);
    }

    private class MissingChunksQueue
    {
        private final LoadingCache<MissingChunk, ListenableFuture<?>> queuedMissingChunks;

        public MissingChunksQueue(PrioritizedFifoExecutor<RecoveryRunnable> executor)
        {
            requireNonNull(executor, "executor is null");
            this.queuedMissingChunks = CacheBuilder.newBuilder().build(new CacheLoader<MissingChunk, ListenableFuture<?>>()
            {
                @Override
                public ListenableFuture<?> load(MissingChunk chunk)
                {
                    Runnable runnable = () -> restoreFromChunkStore(chunk.getChunkId(), chunk.getSize(), chunk.getXxhash64());
                    ListenableFuture<?> future = executor.submit(new RecoveryRunnable(chunk.isActive(), runnable));
                    future.addListener(() -> queuedMissingChunks.invalidate(chunk), directExecutor());
                    return future;
                }
            });
        }

        public ListenableFuture<?> submit(MissingChunk chunk)
        {
            return queuedMissingChunks.getUnchecked(chunk);
        }
    }

    private static class RecoveryRunnable
            implements Runnable
    {
        private final boolean active;
        private final Runnable runnable;

        public RecoveryRunnable(boolean active, Runnable runnable)
        {
            this.active = active;
            this.runnable = requireNonNull(runnable, "runnable is null");
        }

        @Override
        public void run()
        {
            runnable.run();
        }

        public static Comparator<RecoveryRunnable> comparator()
        {
            return comparingInt(value -> value.active ? 0 : 1);
        }
    }

    private static final class MissingChunk
    {
        private final boolean active;
        private final long chunkId;
        private final long size;
        private final long xxhash64;

        public MissingChunk(boolean active, long chunkId, long size, long xxhash64)
        {
            this.active = active;
            this.chunkId = chunkId;
            this.size = size;
            this.xxhash64 = xxhash64;
        }

        public boolean isActive()
        {
            return active;
        }

        public long getChunkId()
        {
            return chunkId;
        }

        public long getSize()
        {
            return size;
        }

        public long getXxhash64()
        {
            return xxhash64;
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

            MissingChunk other = (MissingChunk) o;
            return Objects.equals(this.chunkId, other.chunkId) &&
                    Objects.equals(this.active, other.active);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(chunkId, active);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("chunkId", chunkId)
                    .add("active", active)
                    .toString();
        }
    }
}
