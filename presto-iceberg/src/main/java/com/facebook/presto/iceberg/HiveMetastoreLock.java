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
package com.facebook.presto.iceberg;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

// JVM mutex short-circuits redundant HMS lock RPCs from the same process; HMS lock provides cross-process serialization.
final class HiveMetastoreLock
        implements AutoCloseable
{
    private static final Logger log = Logger.get(HiveMetastoreLock.class);
    private static final long LOCK_CACHE_EVICTION_MILLIS = TimeUnit.MINUTES.toMillis(10);

    private static LoadingCache<String, ReentrantLock> jvmLockCache;

    private final ExtendedHiveMetastore metastore;
    private final MetastoreContext metastoreContext;
    private final String databaseName;
    private final String objectName;
    private final ReentrantLock jvmMutex;
    private final Optional<Long> hmsLockId;

    private HiveMetastoreLock(
            ExtendedHiveMetastore metastore,
            MetastoreContext metastoreContext,
            String databaseName,
            String objectName,
            ReentrantLock jvmMutex,
            Optional<Long> hmsLockId)
    {
        this.metastore = metastore;
        this.metastoreContext = metastoreContext;
        this.databaseName = databaseName;
        this.objectName = objectName;
        this.jvmMutex = jvmMutex;
        this.hmsLockId = hmsLockId;
    }

    static HiveMetastoreLock acquire(
            ExtendedHiveMetastore metastore,
            MetastoreContext metastoreContext,
            boolean useHmsLock,
            String databaseName,
            String objectName)
    {
        requireNonNull(metastore, "metastore is null");
        requireNonNull(metastoreContext, "metastoreContext is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(objectName, "objectName is null");

        initJvmLockCache();
        ReentrantLock mutex = jvmLockCache.getUnchecked(databaseName + "." + objectName);
        mutex.lock();
        Optional<Long> lockId = Optional.empty();
        try {
            if (useHmsLock) {
                lockId = metastore.lock(metastoreContext, databaseName, objectName);
            }
        }
        catch (Throwable e) {
            mutex.unlock();
            throw e;
        }
        return new HiveMetastoreLock(metastore, metastoreContext, databaseName, objectName, mutex, lockId);
    }

    @Override
    public void close()
    {
        try {
            hmsLockId.ifPresent(id -> metastore.unlock(metastoreContext, id));
        }
        catch (Exception e) {
            log.error(e, "Failed to unlock %s.%s (lockId=%s)", databaseName, objectName, hmsLockId.orElse(null));
        }
        finally {
            jvmMutex.unlock();
        }
    }

    // TODO: derive from config
    private static synchronized void initJvmLockCache()
    {
        if (jvmLockCache == null) {
            jvmLockCache = CacheBuilder.newBuilder()
                    .expireAfterAccess(LOCK_CACHE_EVICTION_MILLIS, TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, ReentrantLock>()
                    {
                        @Override
                        public ReentrantLock load(String fullName)
                        {
                            return new ReentrantLock();
                        }
                    });
        }
    }
}
