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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class BackgroundBatchCacheLoader<K, V>
        extends CacheLoader<K, V>
{
    private static final Logger logger = Logger.get(BackgroundBatchCacheLoader.class);
    private final ConcurrentMap<K, SettableFuture<V>> pendingReloadMap = Maps.newConcurrentMap();
    private final int batchLoadSize;
    private final ScheduledExecutorService scheduledExecutorService;
    private final long batchLoadIntervalMillis;
    private volatile boolean started = false;

    protected BackgroundBatchCacheLoader(long batchLoadIntervalMillis, int batchLoadSize, ScheduledExecutorService scheduledExecutorService)
    {
        checkArgument(batchLoadIntervalMillis > 0, "batchLoadIntervalMillis must be a positive integer.");
        checkArgument(batchLoadSize > 0, "batchLoadSize must be a positive integer.");
        checkNotNull(scheduledExecutorService, "scheduledExecutorService cannot be null.");
        this.batchLoadIntervalMillis = batchLoadIntervalMillis;
        this.batchLoadSize = batchLoadSize;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public synchronized void start()
    {
        if (!started) {
            Runnable timeThresholdReachedRunnable = new Runnable()
            {
                @Override
                public void run()
                {
                    ConcurrentMap<K, SettableFuture<V>> refreshingMap = emptyPendingMap(true);
                    refresh(refreshingMap);
                }
            };
            scheduledExecutorService.scheduleWithFixedDelay(timeThresholdReachedRunnable, batchLoadIntervalMillis, batchLoadIntervalMillis, MILLISECONDS);
            started = true;
        }
    }

    private void refresh(ConcurrentMap<K, SettableFuture<V>> refreshingMap)
    {
        try {
            while (refreshingMap != null && refreshingMap.size() > 0) {
                List<K> keys = refreshingMap.keySet().stream().limit(batchLoadSize).collect(Collectors.toList());
                Map<K, V> values = loadAll(keys);
                for (Map.Entry<K, V> entry : values.entrySet()) {
                    SettableFuture<V> settableFuture = refreshingMap.remove(entry.getKey());
                    settableFuture.set(entry.getValue());
                }
            }
        }
        catch (Exception t) {
            // loadAll may throw Exception.
            logger.error(t);
        }
    }

    private synchronized ConcurrentMap<K, SettableFuture<V>> emptyPendingMap(boolean ignoreSize)
    {
        int minimumSize = ignoreSize ? 1 : batchLoadSize;
        if (pendingReloadMap.size() >= minimumSize) {
            ConcurrentMap<K, SettableFuture<V>> pendingReloadMapSnapshot = Maps.newConcurrentMap();
            pendingReloadMapSnapshot.putAll(pendingReloadMap);
            pendingReloadMap.keySet().removeAll(pendingReloadMapSnapshot.keySet());
            return pendingReloadMapSnapshot;
        }
        return null;
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
        SettableFuture<V> existingFuture = pendingReloadMap.putIfAbsent(key, newFuture);

        if (existingFuture == null) {
            existingFuture = newFuture;
            if (pendingReloadMap.size() >= batchLoadSize) {
                scheduledExecutorService.submit(
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                // get a snapshot and refresh the snapshot
                                ConcurrentMap<K, SettableFuture<V>> refreshingMap = emptyPendingMap(true);
                                refresh(refreshingMap);
                            }
                        });
            }
        }

        return existingFuture;
    }
}
