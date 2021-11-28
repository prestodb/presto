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
package com.facebook.presto.cache.alluxio;

import alluxio.client.file.cache.CacheManager;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.io.FileUtils;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.CacheQuota;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.cache.CacheType.ALLUXIO;
import static com.facebook.presto.cache.TestingCacheUtils.stressTest;
import static com.facebook.presto.cache.TestingCacheUtils.validateBuffer;
import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static com.facebook.presto.hive.CacheQuotaScope.TABLE;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.nio.file.Files.createTempDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestAlluxioCachingFileSystem
{
    private static final int DATA_LENGTH = (int) new DataSize(20, KILOBYTE).toBytes();
    private static final int PAGE_SIZE = (int) new DataSize(1, KILOBYTE).toBytes();
    private final byte[] data = new byte[DATA_LENGTH];
    private URI cacheDirectory;
    private String testFilePath;
    private Map<String, Long> baseline = new HashMap<>();

    @BeforeClass
    public void setup()
            throws IOException
    {
        new Random().nextBytes(data);
        this.cacheDirectory = createTempDirectory("alluxio_cache").toUri();
    }

    @AfterClass
    public void close()
            throws IOException
    {
        checkState(cacheDirectory != null);
        FileUtils.deletePathRecursively(cacheDirectory.getPath());
    }

    @BeforeMethod
    public void setupMethod()
    {
        // This path is only used for in memory stream without file materialized.
        testFilePath = String.format("/test/file_%d", new Random().nextLong());
        resetBaseline();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        // Cleanup CacheManager singleton to prevent state leftover across tests
        resetCacheManager();
    }

    @Test(timeOut = 30_000)
    public void testBasicWithValidationDisabled()
            throws Exception
    {
        testBasic(false);
    }

    private void testBasic(boolean validationEnabled)
            throws Exception
    {
        CacheConfig cacheConfig = new CacheConfig()
                .setCacheType(ALLUXIO)
                .setCachingEnabled(true)
                .setBaseDirectory(cacheDirectory)
                .setValidationEnabled(validationEnabled);
        AlluxioCacheConfig alluxioCacheConfig = new AlluxioCacheConfig();
        Configuration configuration = getHdfsConfiguration(cacheConfig, alluxioCacheConfig);
        AlluxioCachingFileSystem fileSystem = cachingFileSystem(configuration);
        byte[] buffer = new byte[PAGE_SIZE * 2];
        int pageOffset = PAGE_SIZE;

        // new read
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + 10, buffer, 0, 100), 100);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 100);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
        validateBuffer(data, pageOffset + 10, buffer, 0, 100);

        // read within the cached page
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + 20, buffer, 0, 90), 90);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 90);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
        validateBuffer(data, pageOffset + 20, buffer, 0, 90);

        // read partially after the range of the cache
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE - 10, buffer, 0, 100), 100);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 10);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 90);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
        validateBuffer(data, pageOffset + PAGE_SIZE - 10, buffer, 0, 100);

        // read partially before the range of the cache
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset - 10, buffer, 10, 50), 50);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 40);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 10);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
        validateBuffer(data, pageOffset - 10, buffer, 10, 50);

        // skip one page
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE * 3, buffer, 40, 50), 50);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 50);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
        validateBuffer(data, pageOffset + PAGE_SIZE * 3, buffer, 40, 50);

        // read between cached pages
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE * 2 - 10, buffer, 400, PAGE_SIZE + 20), PAGE_SIZE + 20);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 20);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
        validateBuffer(data, pageOffset + PAGE_SIZE * 2 - 10, buffer, 400, PAGE_SIZE + 20);
    }

    @Test(invocationCount = 10)
    public void testStress()
            throws ExecutionException, InterruptedException, URISyntaxException, IOException
    {
        CacheConfig cacheConfig = new CacheConfig()
                .setCacheType(ALLUXIO)
                .setCachingEnabled(true)
                .setBaseDirectory(cacheDirectory);
        AlluxioCacheConfig alluxioCacheConfig = new AlluxioCacheConfig()
                .setMaxCacheSize(new DataSize(10, KILOBYTE));
        Configuration configuration = getHdfsConfiguration(cacheConfig, alluxioCacheConfig);
        AlluxioCachingFileSystem cachingFileSystem = cachingFileSystem(configuration);
        stressTest(data, (position, buffer, offset, length) -> {
            try {
                readFully(cachingFileSystem, position, buffer, offset, length);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test(timeOut = 30_000, expectedExceptions = {IOException.class})
    public void testSyncRestoreFailure()
            throws Exception
    {
        URI badCacheDirectory = createTempDirectory("alluxio_cache_bad").toUri();
        File cacheDirectory = new File(badCacheDirectory.getPath());
        cacheDirectory.setWritable(false);
        CacheConfig cacheConfig = new CacheConfig()
                .setCacheType(ALLUXIO)
                .setCachingEnabled(true)
                .setBaseDirectory(badCacheDirectory);
        AlluxioCacheConfig alluxioCacheConfig = new AlluxioCacheConfig();
        Configuration configuration = getHdfsConfiguration(cacheConfig, alluxioCacheConfig);
        try {
            cachingFileSystem(configuration);
        }
        finally {
            cacheDirectory.setWritable(true);
        }
    }

    @Test(timeOut = 30_000)
    public void testBasicReadWithAsyncRestoreFailure()
            throws Exception
    {
        File cacheDirectory = new File(this.cacheDirectory.getPath());
        cacheDirectory.setWritable(false);
        CacheConfig cacheConfig = new CacheConfig()
                .setCacheType(ALLUXIO)
                .setCachingEnabled(true)
                .setBaseDirectory(this.cacheDirectory);
        AlluxioCacheConfig alluxioCacheConfig = new AlluxioCacheConfig();
        Configuration configuration = getHdfsConfiguration(cacheConfig, alluxioCacheConfig);
        configuration.set("alluxio.user.client.cache.async.restore.enabled", String.valueOf(true));
        try {
            AlluxioCachingFileSystem fileSystem = cachingFileSystem(configuration);
            long state = MetricsSystem.counter(MetricKey.CLIENT_CACHE_STATE.getName()).getCount();
            assertTrue(state == CacheManager.State.READ_ONLY.getValue() || state == CacheManager.State.NOT_IN_USE.getValue());
            // different cases of read can still proceed even cache is read-only or not-in-use
            byte[] buffer = new byte[PAGE_SIZE * 2];
            int pageOffset = PAGE_SIZE;
            // new read
            resetBaseline();
            assertEquals(readFully(fileSystem, pageOffset + 10, buffer, 0, 100), 100);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 100);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
            validateBuffer(data, pageOffset + 10, buffer, 0, 100);

            // read within the cached page
            resetBaseline();
            assertEquals(readFully(fileSystem, pageOffset + 20, buffer, 0, 90), 90);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 90);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
            validateBuffer(data, pageOffset + 20, buffer, 0, 90);

            // read partially after the range of the cache
            resetBaseline();
            assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE - 10, buffer, 0, 100), 100);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 100);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, 2 * PAGE_SIZE);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
            validateBuffer(data, pageOffset + PAGE_SIZE - 10, buffer, 0, 100);

            // read partially before the range of the cache
            resetBaseline();
            assertEquals(readFully(fileSystem, pageOffset - 10, buffer, 10, 50), 50);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 50);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, 2 * PAGE_SIZE);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
            validateBuffer(data, pageOffset - 10, buffer, 10, 50);

            // skip one page
            resetBaseline();
            assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE * 3, buffer, 40, 50), 50);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 50);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
            validateBuffer(data, pageOffset + PAGE_SIZE * 3, buffer, 40, 50);

            // read between cached pages
            resetBaseline();
            assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE * 2 - 10, buffer, 400, PAGE_SIZE + 20), PAGE_SIZE + 20);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, PAGE_SIZE + 20);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, 3 * PAGE_SIZE);
            checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
            validateBuffer(data, pageOffset + PAGE_SIZE * 2 - 10, buffer, 400, PAGE_SIZE + 20);

            state = MetricsSystem.counter(MetricKey.CLIENT_CACHE_STATE.getName()).getCount();
            assertTrue(state == CacheManager.State.READ_ONLY.getValue() || state == CacheManager.State.NOT_IN_USE.getValue());
        }
        finally {
            cacheDirectory.setWritable(true);
        }
    }

    @Test(timeOut = 30_000)
    public void testQuotaBasics()
            throws Exception
    {
        DataSize quotaSize = DataSize.succinctDataSize(1, KILOBYTE);
        CacheQuota cacheQuota = new CacheQuota("test.table", Optional.of(quotaSize));
        CacheConfig cacheConfig = new CacheConfig()
                .setCacheType(ALLUXIO)
                .setCachingEnabled(true)
                .setBaseDirectory(cacheDirectory)
                .setValidationEnabled(false)
                .setCacheQuotaScope(TABLE);
        AlluxioCacheConfig alluxioCacheConfig = new AlluxioCacheConfig().setCacheQuotaEnabled(true);
        Configuration configuration = getHdfsConfiguration(cacheConfig, alluxioCacheConfig);
        AlluxioCachingFileSystem fileSystem = cachingFileSystem(configuration);

        byte[] buffer = new byte[10240];

        // read within the cache quota
        resetBaseline();
        assertEquals(readFully(fileSystem, cacheQuota, 42, buffer, 0, 100), 100);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 100);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
        validateBuffer(data, 42, buffer, 0, 100);

        // read beyond cache quota
        resetBaseline();
        assertEquals(readFully(fileSystem, cacheQuota, 47, buffer, 0, 9000), 9000);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, PAGE_SIZE - 47);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 9000 - PAGE_SIZE + 47);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, (9000 / PAGE_SIZE) * PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, (9000 / PAGE_SIZE) * PAGE_SIZE);
        validateBuffer(data, 47, buffer, 0, 9000);
    }

    @Test(timeOut = 30_000)
    public void testQuotaUpdated()
            throws Exception
    {
        CacheQuota smallCacheQuota = new CacheQuota("test.table", Optional.of(DataSize.succinctDataSize(1, KILOBYTE)));
        CacheConfig cacheConfig = new CacheConfig()
                .setCacheType(ALLUXIO)
                .setCachingEnabled(true)
                .setBaseDirectory(cacheDirectory)
                .setValidationEnabled(false)
                .setCacheQuotaScope(TABLE);
        AlluxioCacheConfig alluxioCacheConfig = new AlluxioCacheConfig().setCacheQuotaEnabled(true);
        Configuration configuration = getHdfsConfiguration(cacheConfig, alluxioCacheConfig);
        AlluxioCachingFileSystem fileSystem = cachingFileSystem(configuration);

        byte[] buffer = new byte[10240];

        // read beyond the small cache quota
        resetBaseline();
        assertEquals(readFully(fileSystem, smallCacheQuota, 0, buffer, 0, 9000), 9000);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 9000);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, (9000 / PAGE_SIZE + 1) * PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, (9000 / PAGE_SIZE) * PAGE_SIZE);
        validateBuffer(data, 0, buffer, 0, 9000);

        // read again within an updated larger cache quota
        CacheQuota largeCacheQuota = new CacheQuota("test.table", Optional.of(DataSize.succinctDataSize(10, KILOBYTE)));
        resetBaseline();
        assertEquals(readFully(fileSystem, largeCacheQuota, 0, buffer, 0, 9000), 9000);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 9000 - (9000 / PAGE_SIZE) * PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, (9000 / PAGE_SIZE) * PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, (9000 / PAGE_SIZE) * PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_EVICTED, 0);
        validateBuffer(data, 0, buffer, 0, 9000);
    }

    @Test(invocationCount = 10)
    public void testStressWithQuota()
            throws ExecutionException, InterruptedException, URISyntaxException, IOException
    {
        CacheQuota cacheQuota = new CacheQuota("test.table", Optional.of(DataSize.succinctDataSize(5, KILOBYTE)));
        CacheConfig cacheConfig = new CacheConfig()
                .setCacheType(ALLUXIO)
                .setCachingEnabled(true)
                .setValidationEnabled(false)
                .setBaseDirectory(cacheDirectory)
                .setCacheQuotaScope(TABLE);
        AlluxioCacheConfig alluxioCacheConfig = new AlluxioCacheConfig()
                .setMaxCacheSize(new DataSize(10, KILOBYTE))
                .setCacheQuotaEnabled(true);
        Configuration configuration = getHdfsConfiguration(cacheConfig, alluxioCacheConfig);
        AlluxioCachingFileSystem cachingFileSystem = cachingFileSystem(configuration);
        stressTest(data, (position, buffer, offset, length) -> {
            try {
                readFully(cachingFileSystem, cacheQuota, position, buffer, offset, length);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test(timeOut = 30_000)
    public void testInitialization()
            throws Exception
    {
        int pageSize = (int) new DataSize(8, KILOBYTE).toBytes();
        int maxCacheSize = (int) new DataSize(512, MEGABYTE).toBytes();
        String jmxClass = "alluxio.metrics.sink.JmxSink";
        String metricsDomain = "com.facebook.alluxio";

        Configuration configuration = new Configuration();
        configuration.set("alluxio.user.local.cache.enabled", "true");
        configuration.set("alluxio.user.client.cache.dir", cacheDirectory.getPath());
        configuration.set("alluxio.user.client.cache.page.size", Integer.toString(pageSize));
        configuration.set("alluxio.user.client.cache.size", Integer.toString(maxCacheSize));
        configuration.set("sink.jmx.class", jmxClass);
        configuration.set("sink.jmx.domain", metricsDomain);

        AlluxioCachingFileSystem fileSystem = cachingFileSystem(configuration);
        Configuration conf = fileSystem.getConf();
        assertTrue(conf.getBoolean("alluxio.user.local.cache.enabled", false));
        assertEquals(cacheDirectory.getPath(), conf.get("alluxio.user.client.cache.dir", "bad result"));
        assertEquals(pageSize, conf.getInt("alluxio.user.client.cache.page.size", 0));
        assertEquals(maxCacheSize, conf.getInt("alluxio.user.client.cache.size", 0));
        assertEquals(jmxClass, conf.get("sink.jmx.class", "bad result"));
        assertEquals(metricsDomain, conf.get("sink.jmx.domain", "bad result"));
    }

    // TODO: update unit tests after CacheManager.reset() is available to avoid using reflection to modify singleton
    private void resetCacheManager()
            throws Exception
    {
        Field field = CacheManager.Factory.class.getDeclaredField("CACHE_MANAGER");
        field.setAccessible(true);
        AtomicReference<CacheManager> managerReference = (AtomicReference<CacheManager>) field.get(null);
        if (managerReference != null) {
            CacheManager manager = managerReference.getAndSet(null);
            if (manager != null) {
                manager.close();
            }
        }
    }

    private void resetBaseline()
    {
        updateBaseline(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE);
        updateBaseline(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL);
        updateBaseline(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL);
        updateBaseline(MetricKey.CLIENT_CACHE_BYTES_EVICTED);
    }

    private void updateBaseline(MetricKey metricsKey)
    {
        baseline.put(metricsKey.getName(), MetricsSystem.meter(metricsKey.getName()).getCount());
    }

    private void checkMetrics(MetricKey metricsKey, long expected)
    {
        assertEquals(MetricsSystem.meter(metricsKey.getName()).getCount() - baseline.getOrDefault(metricsKey.getName(), 0L), expected);
    }

    private AlluxioCachingFileSystem cachingFileSystem(Configuration configuration)
            throws URISyntaxException, IOException
    {
        Map<Path, byte[]> files = new HashMap<>();
        files.put(new Path(testFilePath), data);
        ExtendedFileSystem testingFileSystem = new TestingFileSystem(files, configuration);
        URI uri = new URI("alluxio://test:8020/");
        AlluxioCachingFileSystem cachingFileSystem = new AlluxioCachingFileSystem(testingFileSystem, uri);
        cachingFileSystem.initialize(uri, configuration);
        return cachingFileSystem;
    }

    private Configuration getHdfsConfiguration(CacheConfig cacheConfig, AlluxioCacheConfig alluxioCacheConfig)
    {
        AlluxioCachingConfigurationProvider provider = new AlluxioCachingConfigurationProvider(cacheConfig, alluxioCacheConfig);
        Configuration configuration = new Configuration();
        provider.updateConfiguration(configuration, null /* ignored */, null /* ignored */);
        if (cacheConfig.isCachingEnabled() && cacheConfig.getCacheType() == ALLUXIO) {
            // we don't have corresponding Presto properties for these two, set them manually
            configuration.set("alluxio.user.client.cache.page.size", Integer.toString(PAGE_SIZE));
            configuration.set("alluxio.user.client.cache.async.restore.enabled", String.valueOf(false));
        }
        return configuration;
    }

    private int readFully(AlluxioCachingFileSystem fileSystem, long position, byte[] buffer, int offset, int length)
            throws Exception
    {
        return readFully(fileSystem, NO_CACHE_CONSTRAINTS, position, buffer, offset, length);
    }

    private int readFully(AlluxioCachingFileSystem fileSystem, CacheQuota quota, long position, byte[] buffer, int offset, int length)
            throws Exception
    {
        try (FSDataInputStream stream = fileSystem.openFile(new Path(testFilePath), new HiveFileContext(true, quota, Optional.empty(), Optional.of((long) DATA_LENGTH), 0, false))) {
            return stream.read(position, buffer, offset, length);
        }
    }

    private static class TestingFileSystem
            extends ExtendedFileSystem
    {
        private final Map<Path, byte[]> files;

        TestingFileSystem(Map<Path, byte[]> files, Configuration configuration)
        {
            this.files = files;
            setConf(configuration);
        }

        @Override
        public FileStatus getFileStatus(Path path)
        {
            if (files.containsKey(path)) {
                return generateURIStatus(path, files.get(path).length);
            }
            return null;
        }

        private FileStatus generateURIStatus(Path path, int length)
        {
            return new FileStatus(length, false, 1, 512, 0, 0, null, null, null, path);
        }

        @Override
        public URI getUri()
        {
            return null;
        }

        @Override
        public FSDataInputStream open(Path path, int bufferSize)
        {
            return new ByteArrayDataInputStream(files.get(path));
        }

        @Override
        public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
        {
            return null;
        }

        @Override
        public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
        {
            return null;
        }

        @Override
        public boolean rename(Path source, Path destination)
        {
            return false;
        }

        @Override
        public boolean delete(Path path, boolean recursive)
        {
            return false;
        }

        @Override
        public FileStatus[] listStatus(Path path)
        {
            return new FileStatus[0];
        }

        @Override
        public void setWorkingDirectory(Path directory)
        {
        }

        @Override
        public Path getWorkingDirectory()
        {
            return null;
        }

        @Override
        public boolean mkdirs(Path path, FsPermission permission)
        {
            return false;
        }

        private static class ByteArrayDataInputStream
                extends FSDataInputStream
        {
            public ByteArrayDataInputStream(byte[] bytes)
            {
                super(new ByteArraySeekableStream(bytes));
            }
        }
    }
}
