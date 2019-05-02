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

import com.facebook.presto.raptorx.metadata.ChunkFile;
import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.spi.NodeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.Sets;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

public class LocalCleaner
{
    private static final Logger log = Logger.get(LocalCleaner.class);

    private final String currentNode;
    private final Ticker ticker;
    private final ChunkManager chunkManager;
    private final StorageService storageService;
    private final Duration localCleanerInterval;
    private final Duration localCleanTime;

    private final AtomicBoolean started = new AtomicBoolean();
    private final ScheduledExecutorService scheduler;

    private final CounterStat localJobErrors = new CounterStat();
    private final CounterStat localChunksCleaned = new CounterStat();

    @GuardedBy("this")
    private final Map<Long, Long> chunksToClean = new HashMap<>();

    @Inject
    public LocalCleaner(
            Ticker ticker,
            NodeManager nodeManager,
            ChunkManager chunkManager,
            StorageService storageService,
            LocalCleanerConfig config)
    {
        this(nodeManager.getCurrentNode().getNodeIdentifier(),
            ticker,
            chunkManager,
            storageService,
            config.getLocalCleanerInterval(),
            config.getLocalCleanTime(),
            config.getThreads());
    }

    public LocalCleaner(
            String currentNode,
            Ticker ticker,
            ChunkManager chunkManager,
            StorageService storageService,
            Duration localCleanerInterval,
            Duration localCleanTime,
            int threads)
    {
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.chunkManager = requireNonNull(chunkManager, "chunkManager is null");
        this.storageService = requireNonNull(storageService, "storageService is null");
        this.localCleanerInterval = requireNonNull(localCleanerInterval, "localCleanerInterval is null");
        this.localCleanTime = requireNonNull(localCleanTime, "localCleanTime is null");
        this.scheduler = newScheduledThreadPool(threads, daemonThreadsNamed("shard-cleaner-%s"));
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            scheduleLocalCleanup();
        }
    }

    @PreDestroy
    public void shutdown()
    {
        scheduler.shutdownNow();
    }

    @Managed
    public void startLocalCleanup()
    {
        scheduler.submit(this::runLocalCleanup);
    }

    @Managed
    public void startLocalCleanupImmediately()
    {
        scheduler.submit(() -> {
            runLocalCleanupImmediately(getLocalChunks());
        });
    }

    private void scheduleLocalCleanup()
    {
        Set<Long> local = getLocalChunks();
        scheduler.submit(() -> {
            waitJitterTime();
            runLocalCleanupImmediately(local);
        });

        scheduler.scheduleWithFixedDelay(() -> {
            waitJitterTime();
            runLocalCleanup();
        }, 0, localCleanerInterval.toMillis(), MILLISECONDS);
    }

    private void waitJitterTime()
    {
        try {
            // jitter to avoid overloading database
            long interval = this.localCleanerInterval.roundTo(SECONDS);
            SECONDS.sleep(ThreadLocalRandom.current().nextLong(1, interval));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @VisibleForTesting
    synchronized Set<Long> getLocalChunks()
    {
        return storageService.getStorageChunks();
    }

    private synchronized void runLocalCleanupImmediately(Set<Long> local)
    {
        try {
            cleanLocalChunksImmediately(local);
        }
        catch (Throwable t) {
            log.error(t, "Error cleaning local chunks");
            localJobErrors.update(1);
        }
    }

    private synchronized void runLocalCleanup()
    {
        try {
            cleanLocalChunks();
        }
        catch (Throwable t) {
            log.error(t, "Error cleaning local chunks");
            localJobErrors.update(1);
        }
    }

    @VisibleForTesting
    synchronized void cleanLocalChunksImmediately(Set<Long> local)
    {
        // get chunks assigned to the local node
        Set<Long> assigned = chunkManager.getNodeChunks(currentNode).stream()
                .map(ChunkFile::getChunkId)
                .collect(toSet());

        // only delete local files that are not assigned
        Set<Long> deletions = Sets.difference(local, assigned);

        for (Long chunkId : deletions) {
            deleteFile(storageService.getStorageFile(chunkId));
        }

        localChunksCleaned.update(deletions.size());
        log.info("Cleaned %s local chunks immediately", deletions.size());
    }

    @VisibleForTesting
    synchronized void cleanLocalChunks()
    {
        // find all files on the local node
        Set<Long> local = getLocalChunks();

        // get chunks assigned to the local node
        Set<Long> assigned = chunkManager.getNodeChunks(currentNode).stream()
                .map(ChunkFile::getChunkId)
                .collect(toSet());

        // un-mark previously marked files that are now assigned
        for (Long chunkId : assigned) {
            chunksToClean.remove(chunkId);
        }

        // mark all files that are not assigned
        for (Long chunkId : local) {
            if (!assigned.contains(chunkId)) {
                chunksToClean.putIfAbsent(chunkId, ticker.read());
            }
        }

        // delete files marked earlier than the clean interval
        long threshold = ticker.read() - localCleanTime.roundTo(NANOSECONDS);
        Set<Long> deletions = chunksToClean.entrySet().stream()
                .filter(entry -> entry.getValue() < threshold)
                .map(Map.Entry::getKey)
                .collect(toSet());
        if (deletions.isEmpty()) {
            return;
        }

        for (Long chunkId : deletions) {
            deleteFile(storageService.getStorageFile(chunkId));
            chunksToClean.remove(chunkId);
        }

        localChunksCleaned.update(deletions.size());
        log.info("Cleaned %s local chunks", deletions.size());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void deleteFile(File file)
    {
        file.delete();
    }

    @Managed
    @Nested
    public CounterStat getLocalChunksCleaned()
    {
        return localChunksCleaned;
    }

    @Managed
    @Nested
    public CounterStat getLocalJobErrors()
    {
        return localJobErrors;
    }
}
