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
package com.facebook.presto.jdbc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.airlift.units.Duration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ReferenceCountingLoadingCache<K, V>
{
    private static class Holder<V>
    {
        private final V value;
        private int referenceCount;
        private int cleanerCount;
        private ScheduledFuture currentCleanup;

        public Holder(V value)
        {
            this.value = requireNonNull(value, "value is null");
        }

        public V get()
        {
            return value;
        }

        public int getReferenceCount()
        {
            return referenceCount;
        }

        public int getCleanerCount()
        {
            return cleanerCount;
        }

        public void reference()
        {
            checkState(referenceCount >= 0, "negative referenceCount in reference()");
            setCleanup(null);
            referenceCount++;
        }

        public void dereference()
        {
            checkState(referenceCount > 0, "non-positive referenceCount in dereference()");
            --referenceCount;
        }

        public boolean isRetained()
        {
            checkState(referenceCount >= 0, "negative referenceCount in isRetained");
            return referenceCount > 0;
        }

        // The purpose of the cleanerCount is explained in the monster comment in PrestoDriver.
        public void setCleanup(ScheduledFuture cleanup)
        {
            if (currentCleanup != null && currentCleanup.cancel(false)) {
                checkState(cleanerCount > 0, "non-positive cleanerCount in setCleanup");
                cleanerCount--;
            }

            checkState(cleanerCount >= 0, "negative cleanerCount in setCleanup");

            currentCleanup = cleanup;
            if (cleanup != null) {
                cleanerCount++;
            }
        }

        public void scheduleCleanup(
                ScheduledExecutorService cleanupService,
                Runnable cleanupTask,
                long delay,
                TimeUnit unit)
        {
            checkState(referenceCount == 0, "scheduleCleanup called while object is still retained");
            ScheduledFuture cleanup = cleanupService.schedule(cleanupTask, delay, unit);
            setCleanup(cleanup);
        }

        public void releaseForCleaning()
        {
            checkState(cleanerCount > 0, "non-positive cleanerCount in releaseForCleaning");
            cleanerCount--;
        }
    }

    private final ScheduledExecutorService valueCleanupService;
    private final LoadingCache<K, Holder<V>> backingCache;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Duration retentionDuration;

    protected ReferenceCountingLoadingCache(
            CacheLoader<K, V> loader,
            Consumer<V> disposer,
            Duration retentionDuration,
            ScheduledExecutorService valueCleanupService)
    {
        requireNonNull(loader, "loader is null");
        requireNonNull(disposer, "disposer is null");
        this.retentionDuration = requireNonNull(retentionDuration, "retentionDuration is null");

        this.valueCleanupService = requireNonNull(valueCleanupService, "valueCleanupService is null");
        this.backingCache = CacheBuilder.newBuilder()
                .removalListener(new RemovalListener<K, Holder<V>>()
                {
                    @Override
                    public void onRemoval(RemovalNotification<K, Holder<V>> notification)
                    {
                        Holder<V> holder = notification.getValue();
                        /*
                         * The docs say that both the key and value may be
                         * null if they've already been garbage collected.
                         * We aren't using weak or soft keys or values, so
                         * this shouldn't apply.
                         */
                        requireNonNull(holder, format("holder is null while removing key %s", notification.getKey()));

                        if (closed.get()) {
                            // Caller goofed.
                            checkState(holder.getReferenceCount() == 0, "Unreleased key %s on close", notification.getKey());
                        }
                        else {
                            // We goofed.
                            checkState(holder.getReferenceCount() == 0, "Non-zero referenceCount disposing %s", notification.getKey());
                            checkState(holder.getCleanerCount() == 0, "Non-zero cleaner count disposing %s", notification.getKey());
                        }

                        disposer.accept(holder.get());
                    }
                })
                .build(new CacheLoader<K, Holder<V>>()
                {
                    @Override
                    public Holder<V> load(K key)
                            throws Exception
                    {
                        return new Holder<>(loader.load(key));
                    }
                });
    }

    public static class Builder<K, V>
    {
        private ScheduledExecutorService valueCleanupService = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("cache-cleanup"));

        private Duration retentionDuration = new Duration(2, TimeUnit.MINUTES);

        public Builder<K, V> withRetentionDuration(Duration retentionDuration)
        {
            this.retentionDuration = requireNonNull(retentionDuration, "retentionDuration is null");
            return this;
        }

        public Builder<K, V> withCleanupService(ScheduledExecutorService valueCleanupService)
        {
            /*
             * We don't need to worry about interrupting anything (or failing to do so).
             * Nothing has been scheduled on the original valueCleanupService.
             */
            this.valueCleanupService.shutdownNow();

            this.valueCleanupService = requireNonNull(valueCleanupService, "valueCleanupService is null");
            return this;
        }

        public ReferenceCountingLoadingCache<K, V> build(CacheLoader<K, V> loader, Consumer<V> disposer)
        {
            return new ReferenceCountingLoadingCache<K, V>(loader, disposer, retentionDuration, valueCleanupService);
        }
    }

    public static <K, V> Builder<K, V> builder()
    {
        return new Builder<>();
    }

    public void close()
    {
        if (closed.compareAndSet(false, true)) {
            synchronized (this) {
                for (Holder<V> holder : backingCache.asMap().values()) {
                    checkState(holder.getReferenceCount() == 0, "Cache closed with outstanding value(s)");
                }
            }
            backingCache.invalidateAll();
            valueCleanupService.shutdown();
        }
    }

    public V acquire(K key)
    {
        checkState(!closed.get(), "Can't acquire from closed cache.");
        synchronized (this) {
            Holder<V> holder = backingCache.getUnchecked(key);
            holder.reference();
            return holder.get();
        }
    }

    public void release(K key)
    {
        checkState(!closed.get(), "Can't release to closed cache.");
        /*
         * Access through the Map interface to avoid creating a Holder and immediately
         * scheduling it for cleanup.
         */
        Holder holder = backingCache.asMap().get(key);
        if (holder == null) {
            return;
        }

        Runnable deferredRelease = () -> {
            synchronized (this) {
                holder.releaseForCleaning();
                if (holder.getCleanerCount() == 0 && holder.getReferenceCount() == 0) {
                    backingCache.invalidate(key);
                }
            }
        };

        synchronized (this) {
            holder.dereference();
            if (!holder.isRetained()) {
                holder.scheduleCleanup(valueCleanupService, deferredRelease, retentionDuration.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    public Duration getRetentionDuration()
    {
        return retentionDuration;
    }
}
