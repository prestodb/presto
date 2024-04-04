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
package com.facebook.presto.operator;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAlluxioFragmentResultCacheManager
{
    private static final String SERIALIZED_PLAN_FRAGMENT_1 = "test plan fragment 1";
    private static final String SERIALIZED_PLAN_FRAGMENT_2 = "test plan fragment 2";
    private static final Split SPLIT_1 = new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new TestingSplit(1));
    private static final Split SPLIT_2 = new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new TestingSplit(2));

    private final ExecutorService writeExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-cache-flusher-%s"));
    private final ExecutorService removalExecutor = newScheduledThreadPool(5, daemonThreadsNamed("test-cache-remover-%s"));
    private final ExecutorService multithreadingWriteExecutor = newScheduledThreadPool(10, daemonThreadsNamed("test-cache-multithreading-flusher-%s"));

    private final AlluxioLocalCluster alluxioLocalCluster;

    private FileSystem fileSystem;

    public TestAlluxioFragmentResultCacheManager()
    {
        alluxioLocalCluster = new AlluxioLocalCluster();
    }

    @BeforeClass
    public void initFileSystem()
    {
        //Configuration.set(PropertyKey.USER_CLIENT_CACHE_ENABLED, true);
        Configuration.set(PropertyKey.USER_SKIP_AUTHORITY_CHECK, true);
        Configuration.set(PropertyKey.USER_CONF_CLUSTER_DEFAULT_ENABLED, false);
        Configuration.set(PropertyKey.MASTER_HOSTNAME, "127.0.0.1");
        Configuration.set(PropertyKey.MASTER_RPC_PORT, alluxioLocalCluster.getMasterRpcPort());
        Configuration.set(PropertyKey.WORKER_RPC_PORT, alluxioLocalCluster.getWorkerRpcPort());
        Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);
        AlluxioConfiguration alluxioConfiguration = Configuration.global();
        FileSystemContext fileSystemContext = FileSystemContext.create(alluxioConfiguration);
        fileSystem = FileSystem.Factory.create(fileSystemContext);
    }

    @AfterClass
    public void close()
            throws IOException, InterruptedException
    {
        writeExecutor.shutdown();
        removalExecutor.shutdown();
        removalExecutor.awaitTermination(30, TimeUnit.SECONDS);
        multithreadingWriteExecutor.shutdown();
        alluxioLocalCluster.close();
    }

    private URI getNewCacheDirectory(String prefix)
            throws Exception
    {
        int port = alluxioLocalCluster.getMasterRpcPort();
        String path = "alluxio://127.0.0.1:" + port + "/" + prefix;
        AlluxioURI dirUri = new AlluxioURI(path);
        if (!fileSystem.exists(dirUri)) {
            fileSystem.createDirectory(dirUri);
        }
        return convertToURI(dirUri);
    }

    private void cleanupCacheDirectory(URI cacheDirectory)
            throws IOException, AlluxioException
    {
        checkState(cacheDirectory != null);
        AlluxioURI alluxioURI = convertToAlluxioURI(cacheDirectory);
        if (!fileSystem.exists(alluxioURI)) {
            return;
        }
        List<URIStatus> files = fileSystem.listStatus(alluxioURI);
        for (URIStatus file : files) {
            fileSystem.delete(new AlluxioURI(file.getPath()));
        }
        fileSystem.delete(alluxioURI);
    }

    private AlluxioURI convertToAlluxioURI(URI uri)
    {
        return new AlluxioURI(uri.getPath());
    }

    private URI convertToURI(AlluxioURI alluxioURI) throws Exception
    {
        return new URI(alluxioURI.toString());
    }

    //@Test(timeOut = 30_000)
    @Test
    public void testBasic()
            throws Exception
    {
        URI cacheDirectory = getNewCacheDirectory("testBasic");
        FragmentCacheStats stats = new FragmentCacheStats();
        AlluxioFragmentResultCacheManager cacheManager = alluxioFragmentResultCacheManager(stats, cacheDirectory);

        // Test fetching new fragment. Current cache status: empty
        assertFalse(cacheManager.get(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_1).isPresent());
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 0);
        assertEquals(stats.getCacheEntries(), 0);
        assertEquals(stats.getCacheSizeInBytes(), 0);

        // Test empty page. Current cache status: empty
        cacheManager.put(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_1, ImmutableList.of()).get();
        Optional<Iterator<Page>> result = cacheManager.get(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_1);
        assertTrue(result.isPresent());
        assertFalse(result.get().hasNext());
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 1);
        assertEquals(stats.getCacheEntries(), 1);
        assertEquals(stats.getCacheSizeInBytes(), 0);

        // Test non-empty page. Current cache status: { (plan1, split1) -> [] }
        List<Page> pages = ImmutableList.of(new Page(createStringsBlock("plan-1-split-2")));
        cacheManager.put(SERIALIZED_PLAN_FRAGMENT_2, SPLIT_2, pages).get();
        result = cacheManager.get(SERIALIZED_PLAN_FRAGMENT_2, SPLIT_2);
        assertTrue(result.isPresent());
        assertPagesEqual(result.get(), pages.iterator());
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 2);
        assertEquals(stats.getCacheEntries(), 2);
        assertEquals(stats.getCacheSizeInBytes(), getCachePhysicalSize(cacheDirectory));

        // Test cache miss for plan mismatch and split mismatch. Current cache status: { (plan1, split1) -> [], (plan2, split2) -> ["plan-1-split-2"] }
        cacheManager.get(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_2);
        assertEquals(stats.getCacheMiss(), 2);
        assertEquals(stats.getCacheHit(), 2);
        assertEquals(stats.getCacheEntries(), 2);
        cacheManager.get(SERIALIZED_PLAN_FRAGMENT_2, SPLIT_1);
        assertEquals(stats.getCacheMiss(), 3);
        assertEquals(stats.getCacheHit(), 2);
        assertEquals(stats.getCacheEntries(), 2);
        assertEquals(stats.getCacheSizeInBytes(), getCachePhysicalSize(cacheDirectory));

        // Test cache invalidation
        cacheManager.invalidateAllCache();
        assertEquals(stats.getCacheMiss(), 3);
        assertEquals(stats.getCacheHit(), 2);
        assertEquals(stats.getCacheEntries(), 0);
        assertEquals(stats.getCacheRemoval(), 2);
        assertEquals(stats.getCacheSizeInBytes(), 0);

        //cleanupCacheDirectory(cacheDirectory);
    }

    @Test(timeOut = 30_000)
    public void testMaxCacheSize()
            throws Exception
    {
        List<Page> pages = ImmutableList.of(new Page(createStringsBlock("plan-1-split-2")));

        URI cacheDirectory = getNewCacheDirectory("testMaxCacheSize");
        FragmentCacheStats stats = new FragmentCacheStats();
        FragmentResultCacheConfig config = new FragmentResultCacheConfig();
        config.setMaxCacheSize(new DataSize(71, DataSize.Unit.BYTE));
        AlluxioFragmentResultCacheManager cacheManager = alluxioFragmentResultCacheManager(stats, config, cacheDirectory);

        // Put one cache entry.
        cacheManager.put(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_1, pages).get();
        Optional<Iterator<Page>> result = cacheManager.get(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_1);
        assertTrue(result.isPresent());
        assertPagesEqual(result.get(), pages.iterator());
        assertEquals(stats.getCacheMiss(), 0);
        assertEquals(stats.getCacheHit(), 1);
        assertEquals(stats.getCacheEntries(), 1);
        assertEquals(stats.getCacheSizeInBytes(), getCachePhysicalSize(cacheDirectory));

        // Trying to add another cache entry which will fail due to total size limit.
        assertNull(cacheManager.put(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_2, pages).get());
        result = cacheManager.get(SERIALIZED_PLAN_FRAGMENT_1, SPLIT_2);
        assertFalse(result.isPresent());
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 1);
        assertEquals(stats.getCacheEntries(), 1);
        assertEquals(stats.getCacheSizeInBytes(), getCachePhysicalSize(cacheDirectory));

        // Adding an empty page is fine.
        cacheManager.put(SERIALIZED_PLAN_FRAGMENT_2, SPLIT_1, ImmutableList.of()).get();
        result = cacheManager.get(SERIALIZED_PLAN_FRAGMENT_2, SPLIT_1);
        assertTrue(result.isPresent());
        assertFalse(result.get().hasNext());
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 2);
        assertEquals(stats.getCacheEntries(), 2);
        assertEquals(stats.getCacheSizeInBytes(), getCachePhysicalSize(cacheDirectory));

        // Test cache invalidation
        cacheManager.invalidateAllCache();
        assertEquals(stats.getCacheMiss(), 1);
        assertEquals(stats.getCacheHit(), 2);
        assertEquals(stats.getCacheEntries(), 0);
        assertEquals(stats.getCacheRemoval(), 2);
        assertEquals(stats.getCacheSizeInBytes(), 0);

        //cleanupCacheDirectory(cacheDirectory);
    }

    private static void assertPagesEqual(Iterator<Page> pages1, Iterator<Page> pages2)
    {
        while (pages1.hasNext() && pages2.hasNext()) {
            Page page1 = pages1.next();
            Page page2 = pages2.next();
            assertEquals(page1.getChannelCount(), page2.getChannelCount());
            for (int i = 0; i < page1.getChannelCount(); i++) {
                assertTrue(page1.getBlock(i).equals(0, 0, page2.getBlock(i), 0, 0, page1.getBlock(0).getSliceLength(0)));
            }
        }
        assertFalse(pages1.hasNext());
        assertFalse(pages2.hasNext());
    }

    @Test(timeOut = 30_000)
    public void testThreadWrite()
            throws Exception
    {
        URI cacheDirectory = getNewCacheDirectory("testThreadWrite");
        String writeThreadNameFormat = "test write content,thread %s,%s";
        FragmentCacheStats stats = new FragmentCacheStats();
        AlluxioFragmentResultCacheManager threadWriteCacheManager = alluxioFragmentResultCacheManager(stats, cacheDirectory);
        ImmutableList.Builder<Future<Boolean>> futures = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            Future<Boolean> future = multithreadingWriteExecutor.submit(() -> {
                try {
                    String threadInfo = String.format(writeThreadNameFormat, Thread.currentThread().getName(), Thread.currentThread().getId());
                    List<Page> pages = ImmutableList.of(new Page(createStringsBlock(threadInfo)));
                    threadWriteCacheManager.put(threadInfo, SPLIT_2, pages).get();
                    Optional<Iterator<Page>> result = threadWriteCacheManager.get(threadInfo, SPLIT_2);
                    assertTrue(result.isPresent());
                    assertPagesEqual(result.get(), pages.iterator());
                    return true;
                }
                catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });
            futures.add(future);
        }
        for (Future<Boolean> future : futures.build()) {
            assertTrue(future.get(30, TimeUnit.SECONDS));
        }

        assertTrue(stats.getCacheSizeInBytes() > 0);
        threadWriteCacheManager.invalidateAllCache();
        assertEquals(stats.getCacheSizeInBytes(), 0);

        //cleanupCacheDirectory(cacheDirectory);
    }

    private boolean test(String writeThreadNameFormat, AlluxioFragmentResultCacheManager threadWriteCacheManager)
    {
        try {
            String threadInfo = String.format(writeThreadNameFormat, Thread.currentThread().getName(), Thread.currentThread().getId());
            List<Page> pages = ImmutableList.of(new Page(createStringsBlock(threadInfo)));
            threadWriteCacheManager.put(threadInfo, SPLIT_2, pages).get();
            Thread.sleep(10000);
            Optional<Iterator<Page>> result = threadWriteCacheManager.get(threadInfo, SPLIT_2);
            assertTrue(result.isPresent());
            assertPagesEqual(result.get(), pages.iterator());
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // Returns the total physical size in bytes for all the cache files.
    private long getCachePhysicalSize(URI cacheDirectory) throws Exception
    {
        checkState(cacheDirectory != null);
        AlluxioURI alluxioURI = convertToAlluxioURI(cacheDirectory);
        List<URIStatus> files = fileSystem.listStatus(alluxioURI);
        long physicalSize = 0;
        for (URIStatus file : files) {
            physicalSize += file.getLength();
        }
        return physicalSize;
    }

    private AlluxioFragmentResultCacheManager alluxioFragmentResultCacheManager(FragmentCacheStats fragmentCacheStats, URI cacheDirectory)
    {
        return alluxioFragmentResultCacheManager(fragmentCacheStats, new FragmentResultCacheConfig(), cacheDirectory);
    }

    private AlluxioFragmentResultCacheManager alluxioFragmentResultCacheManager(FragmentCacheStats fragmentCacheStats, FragmentResultCacheConfig cacheConfig, URI cacheDirectory)
    {
        return new AlluxioFragmentResultCacheManager(
                cacheConfig.setBaseDirectory(cacheDirectory),
                new TestingBlockEncodingSerde(),
                fragmentCacheStats,
                writeExecutor,
                removalExecutor);
    }

    private static class TestingSplit
            implements ConnectorSplit
    {
        private final int id;

        public TestingSplit(int id)
        {
            this.id = id;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return NO_PREFERENCE;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return this;
        }

        @Override
        public Object getSplitIdentifier()
        {
            return id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingSplit that = (TestingSplit) o;
            return id == that.id;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .toString();
        }
    }
}
