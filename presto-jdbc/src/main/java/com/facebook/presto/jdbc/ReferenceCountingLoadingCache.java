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

import com.google.common.base.Joiner;
import io.airlift.units.Duration;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

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

    private final Function<K, V> loader;
    private final Consumer<V> disposer;
    private final ScheduledExecutorService valueCleanupService;
    private final HashMap<K, Holder<V>> backingCache;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Duration retentionDuration;

    protected ReferenceCountingLoadingCache(
            Function<K, V> loader,
            Consumer<V> disposer,
            Duration retentionDuration,
            ScheduledExecutorService valueCleanupService)
    {
        this.loader = requireNonNull(loader, "loader is null");
        this.disposer = requireNonNull(disposer, "disposer is null");
        this.retentionDuration = requireNonNull(retentionDuration, "retentionDuration is null");

        this.valueCleanupService = requireNonNull(valueCleanupService, "valueCleanupService is null");
        this.backingCache = new HashMap<>();
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

        public ReferenceCountingLoadingCache<K, V> build(Function<K, V> loader, Consumer<V> disposer)
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
            valueCleanupService.shutdown();
            synchronized (this) {
                List<K> unreleased = new LinkedList<>();
                for (Map.Entry<K, Holder<V>> entry : backingCache.entrySet()) {
                    if (entry.getValue().getReferenceCount() != 0) {
                        unreleased.add(entry.getKey());
                    }
                    removeFromCache(entry.getKey(), entry.getValue());
                }
                checkState(unreleased.size() == 0, format(
                        "Unreleased objects in cache at close. Keys: %s", Joiner.on(", ").join(unreleased)));
            }
        }
    }

    public V acquire(K key)
    {
        checkState(!closed.get(), "Can't acquire from closed cache.");
        synchronized (this) {
            Holder<V> holder = backingCache.computeIfAbsent(key, loaderKey -> new Holder<V>(loader.apply(loaderKey)));
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
        Holder<V> holder = backingCache.get(key);
        if (holder == null) {
            return;
        }

        Runnable deferredRelease = () -> {
            synchronized (this) {
                removeFromCache(key, holder);
            }
        };

        synchronized (this) {
            holder.dereference();
            if (!holder.isRetained()) {
                holder.scheduleCleanup(valueCleanupService, deferredRelease, retentionDuration.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private void removeFromCache(K key, Holder<V> holder)
    {
        if (!closed.get()) {
            holder.releaseForCleaning();
            if (!(holder.getCleanerCount() == 0 && holder.getReferenceCount() == 0)) {
                return;
            }
        }

        backingCache.remove(key);

        if (!closed.get()) {
            // We goofed.
            checkState(holder.getReferenceCount() == 0, "Non-zero reference count disposing %s", key);
            checkState(holder.getCleanerCount() == 0, "Non-zero cleaner count disposing %s", key);
        }

        disposer.accept(holder.get());
    }

    public Duration getRetentionDuration()
    {
        return retentionDuration;
    }
}
