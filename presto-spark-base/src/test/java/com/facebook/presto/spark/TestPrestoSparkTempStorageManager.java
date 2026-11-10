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
package com.facebook.presto.spark;

import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageContext;
import com.facebook.presto.spi.storage.TempStorageFactory;
import com.facebook.presto.spiller.LocalTempStorage;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.facebook.presto.spark.PrestoSparkTempStorageManager.shouldDeferTempStorages;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.DRIVER;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.EXECUTOR;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.LOCAL_EXECUTOR;
import static com.facebook.presto.spiller.LocalTempStorage.TEMP_STORAGE_PATH;
import static com.facebook.presto.storage.TempStorageManager.TEMP_STORAGE_FACTORY_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoSparkTempStorageManager
{
    private Path tempDirectory;

    @BeforeClass
    public void setUp()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("presto-spark-temp-storage-manager");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDirectory, ALLOW_INSECURE);
    }

    @Test
    public void testShouldDeferTempStorages()
    {
        // Only native executors defer, and only when spark.defer-native-executor-temp-storage is true
        assertTrue(shouldDeferTempStorages(EXECUTOR, true, true));
        assertFalse(shouldDeferTempStorages(EXECUTOR, true, false));
        assertFalse(shouldDeferTempStorages(EXECUTOR, false, true));
        assertFalse(shouldDeferTempStorages(DRIVER, true, true));
        assertFalse(shouldDeferTempStorages(LOCAL_EXECUTOR, true, true));
    }

    @Test
    public void testDeferredTempStorageIsCreatedOnFirstUse()
            throws IOException
    {
        CountingTempStorageFactory factory = new CountingTempStorageFactory(tempDirectory.resolve("first-use"));
        PrestoSparkTempStorageManager manager = new PrestoSparkTempStorageManager(new TestingNodeManager(), true);
        manager.addTempStorageFactory(factory);

        manager.loadTempStorages(ImmutableMap.of("scratch", factory.properties()));
        assertEquals(factory.created(), 0);

        TempStorage tempStorage = manager.getTempStorage("scratch");
        assertEquals(factory.created(), 1);
        assertSame(manager.getTempStorage("scratch"), tempStorage);
        assertEquals(factory.created(), 1);
    }

    @Test
    public void testTempStorageIsCreatedAtLoadWhenNotDeferred()
            throws IOException
    {
        CountingTempStorageFactory factory = new CountingTempStorageFactory(tempDirectory.resolve("not-deferred"));
        PrestoSparkTempStorageManager manager = new PrestoSparkTempStorageManager(new TestingNodeManager(), false);
        manager.addTempStorageFactory(factory);

        manager.loadTempStorages(ImmutableMap.of("scratch", factory.properties()));
        assertEquals(factory.created(), 1);
        manager.getTempStorage("scratch");
        assertEquals(factory.created(), 1);
    }

    @Test
    public void testUnknownTempStorageStillFails()
            throws IOException
    {
        CountingTempStorageFactory factory = new CountingTempStorageFactory(tempDirectory.resolve("unknown"));
        PrestoSparkTempStorageManager manager = new PrestoSparkTempStorageManager(new TestingNodeManager(), true);
        manager.addTempStorageFactory(factory);
        manager.loadTempStorages(ImmutableMap.of("scratch", factory.properties()));

        assertThrows(IllegalStateException.class, () -> manager.getTempStorage("missing"));
        assertEquals(factory.created(), 0);
    }

    @Test
    public void testLocalTempStorageIsNotDeferred()
            throws IOException
    {
        PrestoSparkTempStorageManager manager = new PrestoSparkTempStorageManager(new TestingNodeManager(), true);
        manager.addTempStorageFactory(new LocalTempStorage.Factory());
        assertFalse(manager.isTempStorageDeferred(LocalTempStorage.NAME));
        assertTrue(manager.isTempStorageDeferred("scratch"));

        // Creating the local temp storage prepares its directory, so it must happen at load time
        Path localPath = tempDirectory.resolve("local");
        manager.loadTempStorages(ImmutableMap.of(
                LocalTempStorage.NAME,
                ImmutableMap.of(TEMP_STORAGE_FACTORY_NAME, LocalTempStorage.NAME, TEMP_STORAGE_PATH, localPath.toString())));
        assertTrue(Files.isDirectory(localPath));
    }

    @Test
    public void testDeferredConfigurationIsValidatedAtLoad()
    {
        CountingTempStorageFactory factory = new CountingTempStorageFactory(tempDirectory.resolve("validated"));

        PrestoSparkTempStorageManager missingFactoryName = new PrestoSparkTempStorageManager(new TestingNodeManager(), true);
        missingFactoryName.addTempStorageFactory(factory);
        assertThrows(IllegalStateException.class, () -> missingFactoryName.loadTempStorages(
                ImmutableMap.of("scratch", ImmutableMap.of(TEMP_STORAGE_PATH, tempDirectory.toString()))));

        PrestoSparkTempStorageManager unregisteredFactory = new PrestoSparkTempStorageManager(new TestingNodeManager(), true);
        unregisteredFactory.addTempStorageFactory(factory);
        assertThrows(IllegalStateException.class, () -> unregisteredFactory.loadTempStorages(
                ImmutableMap.of("scratch", ImmutableMap.of(TEMP_STORAGE_FACTORY_NAME, "unregistered"))));

        assertEquals(factory.created(), 0);
    }

    @Test
    public void testConcurrentFirstUseCreatesOnce()
            throws IOException, InterruptedException
    {
        CountDownLatch creationStarted = new CountDownLatch(1);
        CountDownLatch releaseCreation = new CountDownLatch(1);
        CountingTempStorageFactory factory = new CountingTempStorageFactory(
                tempDirectory.resolve("concurrent"),
                creationStarted,
                releaseCreation);
        PrestoSparkTempStorageManager manager = new PrestoSparkTempStorageManager(new TestingNodeManager(), true);
        manager.addTempStorageFactory(factory);
        manager.loadTempStorages(ImmutableMap.of("scratch", factory.properties()));

        int threadCount = 8;
        AtomicReferenceArray<TempStorage> results = new AtomicReferenceArray<>(threadCount);
        AtomicReferenceArray<Throwable> failures = new AtomicReferenceArray<>(threadCount);
        List<Thread> threads = ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7).stream()
                .map(index -> new Thread(() -> {
                    try {
                        results.set(index, manager.getTempStorage("scratch"));
                    }
                    catch (Throwable t) {
                        failures.set(index, t);
                    }
                }))
                .collect(toImmutableList());
        threads.forEach(Thread::start);

        // Hold the first creation until every thread is waiting, so all first uses overlap
        assertTrue(creationStarted.await(10, SECONDS));
        awaitAllWaiting(threads);
        releaseCreation.countDown();
        for (Thread thread : threads) {
            thread.join(SECONDS.toMillis(10));
            assertFalse(thread.isAlive());
        }

        for (int i = 0; i < threadCount; i++) {
            assertNull(failures.get(i));
            assertSame(results.get(i), results.get(0));
        }
        assertEquals(factory.created(), 1);
    }

    private static void awaitAllWaiting(List<Thread> threads)
    {
        long deadline = System.nanoTime() + SECONDS.toNanos(10);
        while (!threads.stream().allMatch(TestPrestoSparkTempStorageManager::isWaiting)) {
            if (System.nanoTime() > deadline) {
                fail("Threads did not reach the temp storage creation in time");
            }
            Thread.onSpinWait();
        }
    }

    private static boolean isWaiting(Thread thread)
    {
        Thread.State state = thread.getState();
        return state == Thread.State.BLOCKED || state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }

    private static class CountingTempStorageFactory
            implements TempStorageFactory
    {
        private final AtomicInteger created = new AtomicInteger();
        private final Path path;
        private final CountDownLatch creationStarted;
        private final CountDownLatch releaseCreation;

        CountingTempStorageFactory(Path path)
        {
            this(path, new CountDownLatch(0), new CountDownLatch(0));
        }

        CountingTempStorageFactory(Path path, CountDownLatch creationStarted, CountDownLatch releaseCreation)
        {
            this.path = path;
            this.creationStarted = creationStarted;
            this.releaseCreation = releaseCreation;
        }

        Map<String, String> properties()
        {
            return ImmutableMap.of(TEMP_STORAGE_FACTORY_NAME, getName(), TEMP_STORAGE_PATH, path.toString());
        }

        int created()
        {
            return created.get();
        }

        @Override
        public String getName()
        {
            return "counting";
        }

        @Override
        public TempStorage create(Map<String, String> config, TempStorageContext context)
        {
            created.incrementAndGet();
            creationStarted.countDown();
            try {
                assertTrue(releaseCreation.await(10, SECONDS));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return new LocalTempStorage.Factory().create(config, context);
        }
    }
}
