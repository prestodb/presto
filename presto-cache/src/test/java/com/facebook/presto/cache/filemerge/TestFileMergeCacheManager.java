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
package com.facebook.presto.cache.filemerge;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheManager;
import com.facebook.presto.cache.CacheStats;
import com.facebook.presto.cache.FileReadRequest;
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
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.cache.TestingCacheUtils.stressTest;
import static com.facebook.presto.cache.TestingCacheUtils.validateBuffer;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFileMergeCacheManager
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
        CacheManager cacheManager = fileMergeCacheManager(stats);
        byte[] buffer = new byte[1024];

        // new read
        assertFalse(readFully(cacheManager, 42, buffer, 0, 100));
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 0);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(data, 42, buffer, 0, 100);

        // within the range of the cache
        assertTrue(readFully(cacheManager, 47, buffer, 0, 90));
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 1);
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(data, 47, buffer, 0, 90);

        // partially within the range of the cache
        assertFalse(readFully(cacheManager, 52, buffer, 0, 100));
        assertEquals(stats.getCacheMiss(), 2);
        assertEquals(stats.getCacheHit(), 1);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(data, 52, buffer, 0, 100);

        // partially within the range of the cache
        assertFalse(readFully(cacheManager, 32, buffer, 10, 50));
        assertEquals(stats.getCacheMiss(), 3);
        assertEquals(stats.getCacheHit(), 1);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(data, 32, buffer, 10, 50);

        // create a hole within two caches
        assertFalse(readFully(cacheManager, 200, buffer, 40, 50));
        assertEquals(stats.getCacheMiss(), 4);
        assertEquals(stats.getCacheHit(), 1);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(data, 200, buffer, 40, 50);

        // use a range to cover the hole
        assertFalse(readFully(cacheManager, 40, buffer, 400, 200));
        assertEquals(stats.getCacheMiss(), 5);
        assertEquals(stats.getCacheHit(), 1);
        stats.trigger();
        assertEquals(stats.getInMemoryRetainedBytes(), 0);
        validateBuffer(data, 40, buffer, 400, 200);
    }

    @Test(invocationCount = 10)
    public void testStress()
            throws ExecutionException, InterruptedException
    {
        CacheConfig cacheConfig = new CacheConfig().setBaseDirectory(cacheDirectory);
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig().setCacheTtl(new Duration(10, MILLISECONDS));

        CacheManager cacheManager = fileMergeCacheManager(cacheConfig, fileMergeCacheConfig);

        stressTest(data, (position, buffer, offset, length) -> readFully(cacheManager, position, buffer, offset, length));
    }

    private CacheManager fileMergeCacheManager(CacheConfig cacheConfig, FileMergeCacheConfig fileMergeCacheConfig)
    {
        return new FileMergeCacheManager(cacheConfig, fileMergeCacheConfig, new CacheStats(), flushExecutor, removeExecutor);
    }

    private CacheManager fileMergeCacheManager(CacheStats cacheStats)
    {
        CacheConfig cacheConfig = new CacheConfig();
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig();
        return new FileMergeCacheManager(cacheConfig.setBaseDirectory(cacheDirectory), fileMergeCacheConfig, cacheStats, flushExecutor, removeExecutor);
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
