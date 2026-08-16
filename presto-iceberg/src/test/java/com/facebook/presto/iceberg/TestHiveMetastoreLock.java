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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.UnimplementedHiveMetastore;
import com.facebook.presto.spi.WarningCollector;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveMetastoreLock
{
    private static final MetastoreContext METASTORE_CONTEXT = new MetastoreContext(
            "test_user",
            "test_query",
            Optional.empty(),
            Collections.emptySet(),
            Optional.empty(),
            Optional.empty(),
            false,
            HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER,
            WarningCollector.NOOP,
            new RuntimeStats());

    @Test
    public void testJvmMutexSerializesConcurrentSections()
            throws Exception
    {
        int threadCount = 4;
        int iterationsPerThread = 10;
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount * iterationsPerThread);

        UnimplementedHiveMetastore metastore = new UnimplementedHiveMetastore();
        try {
            for (int i = 0; i < threadCount * iterationsPerThread; i++) {
                executor.submit(() -> {
                    try {
                        start.await();
                        try (HiveMetastoreLock ignored = HiveMetastoreLock.acquire(metastore, METASTORE_CONTEXT, false, "db", "obj")) {
                            int current = inFlight.incrementAndGet();
                            maxInFlight.updateAndGet(previous -> Math.max(previous, current));
                            Thread.sleep(2);
                            inFlight.decrementAndGet();
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "sections did not complete in time");
        }
        finally {
            executor.shutdownNow();
        }

        assertEquals(maxInFlight.get(), 1, "lock failed to serialize: max concurrent in-flight = " + maxInFlight.get());
    }

    @Test
    public void testHmsLockAcquiredAndReleasedWhenEnabled()
    {
        AtomicInteger lockCount = new AtomicInteger();
        AtomicInteger unlockCount = new AtomicInteger();
        UnimplementedHiveMetastore metastore = new UnimplementedHiveMetastore()
        {
            @Override
            public Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
            {
                return Optional.of((long) lockCount.incrementAndGet());
            }

            @Override
            public void unlock(MetastoreContext metastoreContext, long lockId)
            {
                unlockCount.incrementAndGet();
            }
        };

        try (HiveMetastoreLock ignored = HiveMetastoreLock.acquire(metastore, METASTORE_CONTEXT, true, "db", "obj_a")) {
            // body intentionally empty — assertions verify lock/unlock side effects on entry/exit
        }
        try (HiveMetastoreLock ignored = HiveMetastoreLock.acquire(metastore, METASTORE_CONTEXT, true, "db", "obj_b")) {
            // body intentionally empty — assertions verify lock/unlock side effects on entry/exit
        }

        assertEquals(lockCount.get(), 2);
        assertEquals(unlockCount.get(), 2);
    }

    @Test
    public void testHmsLockNotAcquiredWhenDisabled()
    {
        AtomicInteger lockCount = new AtomicInteger();
        AtomicInteger unlockCount = new AtomicInteger();
        UnimplementedHiveMetastore metastore = new UnimplementedHiveMetastore()
        {
            @Override
            public Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
            {
                lockCount.incrementAndGet();
                return Optional.of(1L);
            }

            @Override
            public void unlock(MetastoreContext metastoreContext, long lockId)
            {
                unlockCount.incrementAndGet();
            }
        };

        try (HiveMetastoreLock ignored = HiveMetastoreLock.acquire(metastore, METASTORE_CONTEXT, false, "db", "obj")) {
            // body intentionally empty — assertion verifies lock() is never called when the flag is off
        }

        assertEquals(lockCount.get(), 0);
        assertEquals(unlockCount.get(), 0);
    }

    @Test
    public void testHmsLockReleasedOnExceptionInProtectedSection()
    {
        AtomicInteger lockCount = new AtomicInteger();
        AtomicInteger unlockCount = new AtomicInteger();
        UnimplementedHiveMetastore metastore = new UnimplementedHiveMetastore()
        {
            @Override
            public Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
            {
                return Optional.of((long) lockCount.incrementAndGet());
            }

            @Override
            public void unlock(MetastoreContext metastoreContext, long lockId)
            {
                unlockCount.incrementAndGet();
            }
        };

        RuntimeException expected = new RuntimeException("boom");
        try {
            try (HiveMetastoreLock ignored = HiveMetastoreLock.acquire(metastore, METASTORE_CONTEXT, true, "db", "obj")) {
                throw expected;
            }
        }
        catch (RuntimeException actual) {
            assertEquals(actual, expected);
        }

        assertEquals(lockCount.get(), 1);
        assertEquals(unlockCount.get(), 1, "unlock must run even when the protected section threw");
    }

    @Test
    public void testJvmMutexReleasedWhenHmsLockAcquisitionFails()
            throws Exception
    {
        AtomicInteger lockAttempts = new AtomicInteger();
        UnimplementedHiveMetastore failingMetastore = new UnimplementedHiveMetastore()
        {
            @Override
            public Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
            {
                lockAttempts.incrementAndGet();
                throw new RuntimeException("hms unavailable");
            }
        };

        try {
            HiveMetastoreLock.acquire(failingMetastore, METASTORE_CONTEXT, true, "db", "obj_fail");
        }
        catch (RuntimeException ignored) {
        }

        // If the JVM mutex had leaked, the next acquire on the same key would block forever.
        CountDownLatch acquired = new CountDownLatch(1);
        Thread acquirer = new Thread(() -> {
            try (HiveMetastoreLock ignored = HiveMetastoreLock.acquire(
                    new UnimplementedHiveMetastore(), METASTORE_CONTEXT, false, "db", "obj_fail")) {
                acquired.countDown();
            }
        });
        acquirer.start();
        assertTrue(acquired.await(5, TimeUnit.SECONDS), "JVM mutex leaked after HMS lock acquisition failure");
        acquirer.join();
        assertEquals(lockAttempts.get(), 1);
    }
}
