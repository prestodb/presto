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
package com.facebook.presto.raptor.backup;

import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ExecutorServiceAdapter;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_TIMEOUT;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TimeoutBackupStore
        implements BackupStore
{
    private final ExecutorService executor;
    private final BackupStore store;

    public TimeoutBackupStore(BackupStore store, String connectorId, Duration timeout, int maxThreads)
    {
        requireNonNull(store, "store is null");
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(timeout, "timeout is null");

        this.executor = newCachedThreadPool(daemonThreadsNamed("backup-proxy-" + connectorId + "-%s"));
        this.store = timeLimited(store, BackupStore.class, timeout, executor, maxThreads);
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Override
    public void backupShard(UUID uuid, File source)
    {
        try {
            store.backupShard(uuid, source);
        }
        catch (UncheckedTimeoutException e) {
            timeoutException(uuid, "Shard backup timed out");
        }
    }

    @Override
    public void restoreShard(UUID uuid, File target)
    {
        try {
            store.restoreShard(uuid, target);
        }
        catch (UncheckedTimeoutException e) {
            timeoutException(uuid, "Shard restore timed out");
        }
    }

    @Override
    public boolean deleteShard(UUID uuid)
    {
        try {
            return store.deleteShard(uuid);
        }
        catch (UncheckedTimeoutException e) {
            throw timeoutException(uuid, "Shard delete timed out");
        }
    }

    @Override
    public boolean shardExists(UUID uuid)
    {
        try {
            return store.shardExists(uuid);
        }
        catch (UncheckedTimeoutException e) {
            throw timeoutException(uuid, "Shard existence check timed out");
        }
    }

    private static <T> T timeLimited(T target, Class<T> clazz, Duration timeout, ExecutorService executor, int maxThreads)
    {
        executor = new ExecutorServiceAdapter(new BoundedExecutor(executor, maxThreads));
        TimeLimiter limiter = new SimpleTimeLimiter(executor);
        return limiter.newProxy(target, clazz, timeout.toMillis(), MILLISECONDS);
    }

    private static PrestoException timeoutException(UUID uuid, String message)
    {
        throw new PrestoException(RAPTOR_BACKUP_TIMEOUT, message + ": " + uuid);
    }
}
