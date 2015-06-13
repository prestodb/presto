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
package com.facebook.presto.hive.util;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class BackgroundBatchCacheLoader<K, V>
        extends CacheLoader<K, V>
{
    private static final Logger logger = Logger.get(BackgroundBatchCacheLoader.class);
    private final ConcurrentMap<K, SettableFuture<V>> futureMap = Maps.newConcurrentMap();
    private final Runnable batchLoader;
    private final AtomicLong lastBatchLoadTime = new AtomicLong();
    private final int batchPartitionLoadSize;
    private final long batchLoadIntervalMillis;

    //let just a single thread do the batch loading
    private final ScheduledExecutorService scheduledExecutorService =
                    Executors.newScheduledThreadPool(1, daemonThreadsNamed("batch-cache-loader-%d"));

    protected BackgroundBatchCacheLoader(final long batchLoadIntervalMillis, final int batchPartitionLoadSize)
    {
        this.batchPartitionLoadSize = batchPartitionLoadSize;
        this.batchLoadIntervalMillis = batchLoadIntervalMillis;
        lastBatchLoadTime.set(System.currentTimeMillis());

        batchLoader = new Runnable() {
            @Override
            public void run()
            {
                try {
                    int partitionsLoaded = 0;
                    while (futureMap.size() > 0) {
                        ImmutableList<K> keys = FluentIterable.from(futureMap.keySet()).limit(batchPartitionLoadSize).toList();
                        Map<K, V> values = loadAll(keys);
                        for (Map.Entry<K, V> entry : values.entrySet()) {
                            SettableFuture<V> f = futureMap.remove(entry.getKey());
                            f.set(entry.getValue());
                        }
                        partitionsLoaded += values.size();
                    }

                    if (partitionsLoaded > 0) {
                        //update lastBatchLoadTime iff we load any partitions
                        lastBatchLoadTime.set(System.currentTimeMillis());
                    }
                }
                catch (Throwable t) {
                    logger.error(t);
                }
            }
        };

        scheduledExecutorService.scheduleAtFixedRate(batchLoader, 0, batchLoadIntervalMillis, MILLISECONDS);
    }

    @Override
    public V load(K key)
            throws Exception
    {
        return loadAll(ImmutableList.of(key)).get(key);
    }

    @Override
    public final ListenableFuture<V> reload(K key, V oldValue)
            throws Exception
    {
        SettableFuture<V> newFuture = SettableFuture.create();
        SettableFuture<V> existingFuture = futureMap.putIfAbsent(key, newFuture);

        if (existingFuture == null) {
            existingFuture = newFuture;
        }

        if (futureMap.size() >= batchPartitionLoadSize) {
            scheduledExecutorService.submit(batchLoader);
        }

        return existingFuture;
    }
}
