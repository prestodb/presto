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

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.io.FileUtils;
import com.facebook.presto.cache.CacheConfig;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.cache.CacheType.ALLUXIO;
import static com.facebook.presto.cache.TestingCacheUtils.stressTest;
import static com.facebook.presto.cache.TestingCacheUtils.validateBuffer;
import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.nio.file.Files.createTempDirectory;
import static org.testng.Assert.assertEquals;

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

    @Test(timeOut = 30_000)
    public void testBasicWithValidationEnabled()
            throws Exception
    {
        testBasic(true);
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
        AlluxioCachingFileSystem fileSystem = cachingFileSystem(cacheConfig, alluxioCacheConfig);
        byte[] buffer = new byte[PAGE_SIZE * 2];
        int pageOffset = PAGE_SIZE;

        // new read
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + 10, buffer, 0, 100), 100);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 100);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        validateBuffer(data, pageOffset + 10, buffer, 0, 100);

        // read within the cached page
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + 20, buffer, 0, 90), 90);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 90);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, 0);
        validateBuffer(data, pageOffset + 20, buffer, 0, 90);

        // read partially after the range of the cache
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE - 10, buffer, 0, 100), 100);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 10);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 90);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        validateBuffer(data, pageOffset + PAGE_SIZE - 10, buffer, 0, 100);

        // read partially before the range of the cache
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset - 10, buffer, 10, 50), 50);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 40);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 10);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        validateBuffer(data, pageOffset - 10, buffer, 10, 50);

        // skip one page
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE * 3, buffer, 40, 50), 50);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 0);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, 50);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
        validateBuffer(data, pageOffset + PAGE_SIZE * 3, buffer, 40, 50);

        // read between cached pages
        resetBaseline();
        assertEquals(readFully(fileSystem, pageOffset + PAGE_SIZE * 2 - 10, buffer, 400, PAGE_SIZE + 20), PAGE_SIZE + 20);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE, 20);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL, PAGE_SIZE);
        checkMetrics(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL, PAGE_SIZE);
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

        AlluxioCachingFileSystem cachingFileSystem = cachingFileSystem(cacheConfig, alluxioCacheConfig);
        stressTest(data, (position, buffer, offset, length) -> {
            try {
                readFully(cachingFileSystem, position, buffer, offset, length);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void resetBaseline()
    {
        updateBaseline(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE);
        updateBaseline(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL);
        updateBaseline(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL);
    }

    private void updateBaseline(MetricKey metricsKey)
    {
        baseline.put(metricsKey.getName(), MetricsSystem.meter(metricsKey.getName()).getCount());
    }

    private void checkMetrics(MetricKey metricsKey, long expected)
    {
        assertEquals(MetricsSystem.meter(metricsKey.getName()).getCount() - baseline.getOrDefault(metricsKey.getName(), 0L), expected);
    }

    private AlluxioCachingFileSystem cachingFileSystem(CacheConfig cacheConfig, AlluxioCacheConfig alluxioCacheConfig)
            throws URISyntaxException, IOException
    {
        Map<Path, byte[]> files = new HashMap<>();
        files.put(new Path(testFilePath), data);
        Configuration configuration = getHdfsConfiguration(cacheConfig, alluxioCacheConfig);
        ExtendedFileSystem testingFileSystem = new TestingFileSystem(files, configuration);
        URI uri = new URI("hdfs://test:8020/");
        AlluxioCachingFileSystem cachingFileSystem = new AlluxioCachingFileSystem(testingFileSystem, uri);
        cachingFileSystem.initialize(uri, configuration);
        return cachingFileSystem;
    }

    private Configuration getHdfsConfiguration(CacheConfig cacheConfig, AlluxioCacheConfig alluxioCacheConfig)
    {
        Configuration configuration = new Configuration();
        if (cacheConfig.isCachingEnabled() && cacheConfig.getCacheType() == ALLUXIO) {
            configuration.set("alluxio.user.local.cache.enabled", String.valueOf(cacheConfig.isCachingEnabled()));
            configuration.set("alluxio.user.client.cache.dir", cacheConfig.getBaseDirectory().getPath());
            configuration.set("alluxio.user.client.cache.size", alluxioCacheConfig.getMaxCacheSize().toString());
            configuration.set("alluxio.user.client.cache.page.size", Integer.toString(PAGE_SIZE));
            configuration.set("alluxio.user.metrics.collection.enabled", String.valueOf(alluxioCacheConfig.isMetricsCollectionEnabled()));
            configuration.set("alluxio.user.client.cache.async.write.enabled", String.valueOf(alluxioCacheConfig.isAsyncWriteEnabled()));
            configuration.set("sink.jmx.class", alluxioCacheConfig.getJmxClass());
            configuration.set("sink.jmx.domain", alluxioCacheConfig.getMetricsDomain());
        }
        return configuration;
    }

    private int readFully(AlluxioCachingFileSystem fileSystem, long position, byte[] buffer, int offset, int length)
            throws Exception
    {
        try (FSDataInputStream stream = fileSystem.openFile(new Path(testFilePath), new HiveFileContext(true, NO_CACHE_CONSTRAINTS, Optional.empty()))) {
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
