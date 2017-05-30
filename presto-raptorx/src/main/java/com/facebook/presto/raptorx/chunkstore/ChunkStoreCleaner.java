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

import com.facebook.presto.raptorx.util.Database;
import com.google.common.collect.ImmutableSet;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.customizer.Define;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ChunkStoreCleaner
{
    private static final Logger log = Logger.get(ChunkStoreCleaner.class);

    private static final int CHUNK_BATCH_SIZE = 1000;

    private final ScheduledExecutorService scheduler = newSingleThreadScheduledExecutor(daemonThreadsNamed("chunk-store-cleaner"));
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ChunkStore chunkStore;
    private final Duration interval;
    private final CleanerDao dao;
    private final AtomicBoolean started = new AtomicBoolean();
    private final CounterStat jobErrors = new CounterStat();
    private final CounterStat deleteFailures = new CounterStat();
    private final CounterStat deleteSuccesses = new CounterStat();

    @Inject
    public ChunkStoreCleaner(ChunkStore chunkStore, Database database, ChunkStoreCleanerConfig config)
    {
        this(chunkStore, database, config.getInterval(), config.getThreads());
    }

    public ChunkStoreCleaner(ChunkStore chunkStore, Database database, Duration interval, int threads)
    {
        this.chunkStore = requireNonNull(chunkStore, "chunkStore is null");
        this.interval = requireNonNull(interval, "interval is null");
        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("chunk-store-cleaner-%s"));
        this.executor = new BoundedExecutor(coreExecutor, threads);
        this.dao = createJdbi(database.getMasterConnection()).onDemand(CleanerDao.class);
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            scheduler.scheduleWithFixedDelay(this::run, 0, interval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        scheduler.shutdownNow();
        coreExecutor.shutdownNow();
    }

    @Managed
    @Nested
    public CounterStat getJobErrors()
    {
        return jobErrors;
    }

    @Managed
    @Nested
    public CounterStat getDeleteFailures()
    {
        return deleteFailures;
    }

    @Managed
    @Nested
    public CounterStat getDeleteSuccesses()
    {
        return deleteSuccesses;
    }

    private void run()
    {
        try {
            cleanChunkStore();
        }
        catch (Throwable t) {
            log.error(t, "Error cleaning chunk store");
            jobErrors.update(1);
        }
    }

    public synchronized void cleanChunkStore()
    {
        Set<Long> processing = newConcurrentHashSet();
        BlockingQueue<Long> completed = new LinkedBlockingQueue<>();
        boolean fill = true;

        while (!Thread.currentThread().isInterrupted()) {
            // get a new batch if any completed and we are under the batch size
            Set<Long> chunkIds = ImmutableSet.of();
            if (fill && (processing.size() < CHUNK_BATCH_SIZE)) {
                chunkIds = dao.getCleanableChunksBatch(System.currentTimeMillis(), CHUNK_BATCH_SIZE);
                fill = false;
            }
            if (chunkIds.isEmpty() && processing.isEmpty()) {
                break;
            }

            // skip any that are already processing and mark remaining as processing
            chunkIds = ImmutableSet.copyOf(difference(chunkIds, processing));
            processing.addAll(chunkIds);

            // execute deletes
            for (long chunkId : chunkIds) {
                runAsync(() -> chunkStore.deleteChunk(chunkId), executor)
                        .thenAccept(v -> completed.add(chunkId))
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                log.error(e, "Error deleting chunk from chunk store: %s", chunkId);
                                deleteFailures.update(1);
                                processing.remove(chunkId);
                            }
                        });
            }

            // get the next batch of completed deletes
            int desired = min(100, processing.size());
            Collection<Long> done = drain(completed, desired, 100, MILLISECONDS);
            if (done.isEmpty()) {
                continue;
            }

            // remove completed deletes from database
            processing.removeAll(done);
            dao.deleteCleanedChunks(done);
            deleteSuccesses.update(done.size());
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
                T value = queue.poll(remainingNanos, NANOSECONDS);
                if (value != null) {
                    result.add(value);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return result;
            }
        }
    }

    public interface CleanerDao
    {
        @SqlQuery("SELECT chunk_id\n" +
                "FROM deleted_chunks\n" +
                "WHERE purge_time <= :purgeTime\n" +
                "LIMIT <limit>")
        Set<Long> getCleanableChunksBatch(
                @Bind long purgeTime,
                @Define int limit);

        @SqlUpdate("DELETE FROM deleted_chunks WHERE chunk_id IN (<chunkIds>)")
        void deleteCleanedChunks(
                @BindList Iterable<Long> chunkIds);
    }
}
