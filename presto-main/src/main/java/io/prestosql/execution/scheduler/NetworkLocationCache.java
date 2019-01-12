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
package io.prestosql.execution.scheduler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.HostAddress;

import java.util.concurrent.ExecutorService;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class NetworkLocationCache
{
    private static final Duration NEGATIVE_CACHE_DURATION = new Duration(10, MINUTES);

    private static final Logger log = Logger.get(NetworkLocationCache.class);

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("network-location-%s"));
    private final NetworkTopology networkTopology;
    private final LoadingCache<HostAddress, NetworkLocation> cache;
    private final Cache<HostAddress, Boolean> negativeCache;

    public NetworkLocationCache(NetworkTopology networkTopology)
    {
        this.networkTopology = requireNonNull(networkTopology, "networkTopology is null");

        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, DAYS)
                .refreshAfterWrite(12, HOURS)
                .build(asyncReloading(CacheLoader.from(this::locate), executor));

        this.negativeCache = CacheBuilder.newBuilder()
                .expireAfterWrite(NEGATIVE_CACHE_DURATION.toMillis(), MILLISECONDS)
                .build();
    }

    public void stop()
    {
        executor.shutdownNow();
    }

    public NetworkLocation get(HostAddress host)
    {
        NetworkLocation location = cache.getIfPresent(host);
        if ((location == null) && (negativeCache.getIfPresent(host) == null)) {
            // Store a value in the cache, so that refresh() is done asynchronously
            cache.put(host, ROOT_LOCATION);
            cache.refresh(host);
        }
        // Return the root location for anything we couldn't locate
        return location == null ? ROOT_LOCATION : location;
    }

    private NetworkLocation locate(HostAddress host)
    {
        try {
            return networkTopology.locate(host);
        }
        catch (RuntimeException e) {
            negativeCache.put(host, true);
            log.warn(e, "Unable to determine location of %s. Will attempt again in %s", host, NEGATIVE_CACHE_DURATION);
            // no one will see the exception thrown here
            throw e;
        }
    }
}
