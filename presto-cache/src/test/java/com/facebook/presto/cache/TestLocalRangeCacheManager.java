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
package com.facebook.presto.cache;

import com.facebook.presto.cache.localrange.LocalRangeCacheConfig;
import com.facebook.presto.cache.localrange.LocalRangeCacheManager;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Integer.max;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestLocalRangeCacheManager
{
    private static final int DATA_LENGTH = (int) new DataSize(20, KILOBYTE).toBytes();
    private final byte[] data = new byte[DATA_LENGTH];
    private final ExecutorService flushExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-cache-flusher-%s"));
    private final ExecutorService removeExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-cache-remover-%s"));

    private URI cacheDirectory;
    private URI fileDirectory;
    private File dataFile;

    @BeforeClass
    public void setup()
            throws IOException
    {
        new Random().nextBytes(data);

        this.cacheDirectory = createTempDirectory("cache").toUri();
        this.fileDirectory = createTempDirectory("file").toUri();
        this.dataFile = new File(fileDirectory.getPath() + "/data");

        Files.write((new File(dataFile.toString())).toPath(), data, CREATE_NEW);
    }

    @AfterClass
    public void close()
            throws IOException
    {
        flushExecutor.shutdown();
        removeExecutor.shutdown();

        checkState(cacheDirectory != null);
        checkState(fileDirectory != null);

        Files.deleteIfExists(dataFile.toPath());
        File[] files = new File(cacheDirectory).listFiles();
        if (files != null) {
            for (File file : files) {
                Files.delete(file.toPath());
            }
        }

        Files.deleteIfExists(new File(cacheDirectory).toPath());
        Files.deleteIfExists(new File(fileDirectory).toPath());
    }

    @Test(timeOut = 30_000)
    public void testBasic()
            throws InterruptedException, ExecutionException, IOException
    {
        TestingCacheStats stats = new TestingCacheStats();
        CacheManager cacheManager = localRangeCacheManager(stats);
        byte[] buffer = new byte[1024];

        // new read
        assertFalse(readFully(cacheManager, 42, buffer, 0, 100));
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 0);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(42, buffer, 0, 100);

        // within the range of the cache
        assertTrue(readFully(cacheManager, 47, buffer, 0, 90));
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 1);
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(47, buffer, 0, 90);

        // partially within the range of the cache
        assertFalse(readFully(cacheManager, 52, buffer, 0, 100));
        assertEquals(stats.getCacheMiss(), 2);
        assertEquals(stats.getCacheHit(), 1);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(52, buffer, 0, 100);

        // partially within the range of the cache
        assertFalse(readFully(cacheManager, 32, buffer, 10, 50));
        assertEquals(stats.getCacheMiss(), 3);
        assertEquals(stats.getCacheHit(), 1);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(32, buffer, 10, 50);

        // create a hole within two caches
        assertFalse(readFully(cacheManager, 200, buffer, 40, 50));
        assertEquals(stats.getCacheMiss(), 4);
        assertEquals(stats.getCacheHit(), 1);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(200, buffer, 40, 50);

        // use a range to cover the hole
        assertFalse(readFully(cacheManager, 40, buffer, 400, 200));
        assertEquals(stats.getCacheMiss(), 5);
        assertEquals(stats.getCacheHit(), 1);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(40, buffer, 400, 200);
    }

    @Test(invocationCount = 10)
    public void testStress()
            throws ExecutionException, InterruptedException
    {
        CacheConfig cacheConfig = new CacheConfig().setBaseDirectory(cacheDirectory);
        LocalRangeCacheConfig localRangeCacheConfig = new LocalRangeCacheConfig().setCacheTtl(new Duration(10, MILLISECONDS));

        CacheManager cacheManager = localRangeCacheManager(cacheConfig, localRangeCacheConfig);

        ExecutorService executor = newScheduledThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();
        AtomicReference<String> exception = new AtomicReference<>();

        for (int i = 0; i < 5; i++) {
            byte[] buffer = new byte[DATA_LENGTH];
            futures.add(executor.submit(() -> {
                Random random = new Random();
                for (int j = 0; j < 200; j++) {
                    int position = random.nextInt(DATA_LENGTH - 1);
                    int length = random.nextInt(max((DATA_LENGTH - position) / 3, 1));
                    int offset = random.nextInt(DATA_LENGTH - length);

                    try {
                        readFully(cacheManager, position, buffer, offset, length);
                    }
                    catch (IOException e) {
                        exception.compareAndSet(null, e.getMessage());
                        return;
                    }
                    validateBuffer(position, buffer, offset, length);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        if (exception.get() != null) {
            fail(exception.get());
        }
    }

    private CacheManager localRangeCacheManager(CacheConfig cacheConfig, LocalRangeCacheConfig localRangeCacheConfig)
    {
        return new LocalRangeCacheManager(cacheConfig, localRangeCacheConfig, new CacheStats(), flushExecutor, removeExecutor);
    }

    private CacheManager localRangeCacheManager(CacheStats cacheStats)
    {
        CacheConfig cacheConfig = new CacheConfig();
        LocalRangeCacheConfig localRangeCacheConfig = new LocalRangeCacheConfig();
        return new LocalRangeCacheManager(cacheConfig.setBaseDirectory(cacheDirectory), localRangeCacheConfig, cacheStats, flushExecutor, removeExecutor);
    }

    private void validateBuffer(long position, byte[] buffer, int offset, int length)
    {
        for (int i = 0; i < length; i++) {
            assertEquals(buffer[i + offset], data[i + (int) position], format("corrupted buffer at position %s offset %s", position, i));
        }
    }

    private boolean readFully(CacheManager cacheManager, long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        FileReadRequest key = new FileReadRequest(new Path(dataFile.getAbsolutePath()), position, length);
        if (!cacheManager.get(key, buffer, offset)) {
            RandomAccessFile file = new RandomAccessFile(dataFile.getAbsolutePath(), "r");
            file.seek(position);
            file.readFully(buffer, offset, length);
            file.close();
            cacheManager.put(key, wrappedBuffer(buffer, offset, length));
            return false;
        }
        return true;
    }

    private static class TestingCacheStats
            extends CacheStats
    {
        private SettableFuture<?> trigger;

        public TestingCacheStats()
        {
            this.trigger = SettableFuture.create();
        }

        @Override
        public void addInMemoryRetainedBytes(long bytes)
        {
            super.addInMemoryRetainedBytes(bytes);
            if (bytes < 0) {
                trigger.set(null);
            }
        }

        public void trigger()
                throws InterruptedException, ExecutionException
        {
            trigger.get();
            trigger = SettableFuture.create();
        }
    }
}
