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

import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ExecutorServiceAdapter;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;

import java.io.File;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_CHUNK_STORE_TIMEOUT;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TimeoutChunkStore
        implements ChunkStore
{
    private final ExecutorService executor;
    private final ChunkStore store;

    public TimeoutChunkStore(ChunkStore store, Duration timeout, int maxThreads)
    {
        requireNonNull(store, "store is null");
        requireNonNull(timeout, "timeout is null");

        this.executor = newCachedThreadPool(daemonThreadsNamed("chunk-store-proxy-%s"));
        this.store = timeLimited(store, ChunkStore.class, timeout, executor, maxThreads);
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Override
    public void putChunk(long chunkId, File source)
    {
        try {
            store.putChunk(chunkId, source);
        }
        catch (UncheckedTimeoutException e) {
            throw timeoutException(chunkId, "Chunk store put timed out");
        }
    }

    @Override
    public void getChunk(long chunkId, File target)
    {
        try {
            store.getChunk(chunkId, target);
        }
        catch (UncheckedTimeoutException e) {
            throw timeoutException(chunkId, "Chunk store get timed out");
        }
    }

    @Override
    public boolean deleteChunk(long chunkId)
    {
        try {
            return store.deleteChunk(chunkId);
        }
        catch (UncheckedTimeoutException e) {
            throw timeoutException(chunkId, "Chunk store delete timed out");
        }
    }

    @Override
    public boolean chunkExists(long chunkId)
    {
        try {
            return store.chunkExists(chunkId);
        }
        catch (UncheckedTimeoutException e) {
            throw timeoutException(chunkId, "Chunk store exists check timed out");
        }
    }

    private static <T> T timeLimited(T target, Class<T> clazz, Duration timeout, ExecutorService executor, int maxThreads)
    {
        executor = new ExecutorServiceAdapter(new BoundedExecutor(executor, maxThreads));
        TimeLimiter limiter = SimpleTimeLimiter.create(executor);
        return limiter.newProxy(target, clazz, timeout.toMillis(), MILLISECONDS);
    }

    private static PrestoException timeoutException(long chunkId, String message)
    {
        return new PrestoException(RAPTOR_CHUNK_STORE_TIMEOUT, message + ": " + chunkId);
    }
}
