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
package com.facebook.presto.raptorx.chunkstore;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.DataSize;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ChunkStoreManager
{
    private final ChunkStore chunkStore;
    private final ListeningExecutorService executorService;

    private final AtomicInteger pendingPuts = new AtomicInteger();
    private final ChunkStoreStats stats = new ChunkStoreStats();

    @Inject
    public ChunkStoreManager(ChunkStore chunkStore, ChunkStoreConfig config)
    {
        this(chunkStore, config.getWriteThreads());
    }

    public ChunkStoreManager(ChunkStore chunkStore, int writeThreads)
    {
        checkArgument(writeThreads > 0, "writeThreads must be > 0");

        this.chunkStore = requireNonNull(chunkStore, "chunkStore is null");
        this.executorService = listeningDecorator(newFixedThreadPool(writeThreads, daemonThreadsNamed("background-chunk-write-%s")));
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
    }

    public ListenableFuture<?> submit(long chunkId, File source)
    {
        requireNonNull(source, "source is null");

        pendingPuts.incrementAndGet();
        long queuedTime = System.nanoTime();

        return executorService.submit(() -> {
            try {
                stats.addQueuedTime(nanosSince(queuedTime));
                runPut(chunkStore, chunkId, source);
            }
            finally {
                pendingPuts.decrementAndGet();
            }
        });
    }

    private void runPut(ChunkStore chunkStore, long chunkId, File source)
    {
        try {
            long start = System.nanoTime();
            chunkStore.putChunk(chunkId, source);
            stats.addCopyChunkDataRate(new DataSize(source.length(), BYTE), nanosSince(start));
            stats.incrementPutSuccess();
        }
        catch (Throwable t) {
            stats.incrementPutFailure();
            throw t;
        }
    }

    @Managed
    public int getPendingPutCount()
    {
        return pendingPuts.get();
    }

    @Managed
    @Flatten
    public ChunkStoreStats getStats()
    {
        return stats;
    }
}
