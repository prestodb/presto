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
package com.facebook.presto.server.protocol;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.spi.QueryId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Objects.requireNonNull;

/*
 * Rate Limiting per query with token bucket
 * Rate = rateLimitBucketMaxSize/second
 * When having sufficient tokens, Request will be responded immediately.
 * When not having enough tokens available, it uses the delayed processing method.
 */
public class QueryBlockingRateLimiter
{
    private final long rateLimiterBucketMaxSize;
    private final ListeningExecutorService rateLimiterExecutorService;
    private final LoadingCache<QueryId, RateLimiter> rateLimiterCache;
    private final CounterStat rateLimiterTriggeredCounter = new CounterStat();
    private final TimeStat rateLimiterBlockTime = new TimeStat();

    @Inject
    public QueryBlockingRateLimiter(QueryManagerConfig queryManagerConfig)
    {
        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.rateLimiterBucketMaxSize = queryManagerConfig.getRateLimiterBucketMaxSize();
        // Using a custom thread pool with size 1-10 to reduce initial thread resources
        ExecutorService executorService = new ThreadPoolExecutor(1, 10, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), daemonThreadsNamed("rate-limiter-listener"));
        rateLimiterExecutorService = listeningDecorator(executorService);
        rateLimiterCache = CacheBuilder.newBuilder().maximumSize(queryManagerConfig.getRateLimiterCacheLimit()).expireAfterAccess(queryManagerConfig.getRateLimiterCacheWindowMinutes(), TimeUnit.MINUTES).build(CacheLoader.from(key -> RateLimiter.create(rateLimiterBucketMaxSize)));
    }

    /*
     * For accidental bug-caused DoS, we will use delayed processing method to reduce the requests, even when user do not have back-off logic implemented
     * Optimized to avoid blocking for normal usages with TryRequire first
     * Fall back to delayed processing method to acquire a permit, in a separate thread pool
     * Internal guava rate limiter returns time spent sleeping to enforce rate, in seconds; 0.0 if not rate-limited, we use a future to wrap around that.
     */
    public ListenableFuture<Double> acquire(QueryId queryId)
    {   // if rateLimitBucketMaxSize < 0, we disable rate limiting by returning immediately
        if (rateLimiterBucketMaxSize < 0) {
            return immediateFuture(0.0);
        }
        if (queryId == null) {
            return immediateFailedFuture(new IllegalArgumentException("queryId should not be null"));
        }
        RateLimiter rateLimiter = rateLimiterCache.getUnchecked(queryId);
        if (rateLimiter.tryAcquire()) {
            return immediateFuture(0.0);
        }
        ListenableFuture<Double> asyncTask = rateLimiterExecutorService.submit(() -> rateLimiter.acquire());
        rateLimiterTriggeredCounter.update(1);
        return asyncTask;
    }

    @Managed
    @Nested
    public CounterStat getRateLimiterTriggeredCounter()
    {
        return rateLimiterTriggeredCounter;
    }

    public TimeStat getRateLimiterBlockTime()
    {
        return rateLimiterBlockTime;
    }

    public void addRateLimiterBlockTime(Duration duration)
    {
        rateLimiterBlockTime.add(duration);
    }

    @PreDestroy
    public void destroy()
    {
        rateLimiterExecutorService.shutdownNow();
    }
}
