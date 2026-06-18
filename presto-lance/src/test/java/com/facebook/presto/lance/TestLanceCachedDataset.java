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
package com.facebook.presto.lance;

import com.facebook.presto.spi.PrestoException;
import com.google.common.io.Resources;
import org.lance.Dataset;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestLanceCachedDataset
{
    // test_table1 has 6 versions in src/test/resources/example_db
    private static final long TABLE1_LATEST_VERSION = 6L;

    private LanceNamespaceHolder namespaceHolder;
    private String table1Path;
    private String table2Path;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL dbUrl = Resources.getResource(TestLanceCachedDataset.class, "/example_db");
        assertNotNull(dbUrl, "example_db resource not found");
        String rootPath = Paths.get(dbUrl.toURI()).toString();
        LanceConfig config = new LanceConfig()
                .setRootUrl(rootPath)
                .setSingleLevelNs(true);
        namespaceHolder = new LanceNamespaceHolder(config);
        table1Path = namespaceHolder.getTablePath("test_table1");
        table2Path = namespaceHolder.getTablePath("test_table2");
    }

    @AfterMethod
    public void tearDown()
    {
        if (namespaceHolder != null) {
            namespaceHolder.shutdown();
        }
    }

    @Test
    public void testEmptyVersionResolvesToLatest()
    {
        Dataset dataset = namespaceHolder.getCachedDataset(table1Path, Optional.empty());
        assertNotNull(dataset);
        assertEquals(dataset.version(), TABLE1_LATEST_VERSION);
    }

    @Test
    public void testSpecificVersionIsHonored()
    {
        Dataset dataset = namespaceHolder.getCachedDataset(table1Path, Optional.of(3L));
        assertNotNull(dataset);
        assertEquals(dataset.version(), 3L);
    }

    @Test
    public void testRepeatedLookupReturnsCachedInstance()
    {
        Dataset first = namespaceHolder.getCachedDataset(table1Path, Optional.of(2L));
        Dataset second = namespaceHolder.getCachedDataset(table1Path, Optional.of(2L));
        assertSame(second, first, "Repeated lookups for the same key must return the cached instance");
    }

    @Test
    public void testDifferentVersionsCacheSeparately()
    {
        Dataset v1 = namespaceHolder.getCachedDataset(table1Path, Optional.of(1L));
        Dataset v2 = namespaceHolder.getCachedDataset(table1Path, Optional.of(2L));
        assertNotSame(v1, v2);
        assertEquals(v1.version(), 1L);
        assertEquals(v2.version(), 2L);
    }

    @Test
    public void testEmptyAndConcreteLatestAreDistinctCacheKeys()
    {
        // Optional.empty() and Optional.of(latest) resolve to the same on-disk
        // version but must hash to different cache keys.
        Dataset latest = namespaceHolder.getCachedDataset(table1Path, Optional.empty());
        Dataset pinned = namespaceHolder.getCachedDataset(table1Path, Optional.of(TABLE1_LATEST_VERSION));
        assertEquals(latest.version(), pinned.version());
        assertNotSame(latest, pinned);
    }

    @Test
    public void testDifferentTablesCacheSeparately()
    {
        Dataset d1 = namespaceHolder.getCachedDataset(table1Path, Optional.empty());
        Dataset d2 = namespaceHolder.getCachedDataset(table2Path, Optional.empty());
        assertNotSame(d1, d2);
    }

    @Test
    public void testMissingTablePathThrowsPrestoException()
    {
        String missingPath = namespaceHolder.getTablePath("does_not_exist");
        try {
            namespaceHolder.getCachedDataset(missingPath, Optional.empty());
            fail("Expected PrestoException for missing table path");
        }
        catch (PrestoException expected) {
            assertEquals(expected.getErrorCode(), LanceErrorCode.LANCE_ERROR.toErrorCode());
            assertNotNull(expected.getCause());
            assertEquals(expected.getCause().getClass(), IllegalArgumentException.class);
        }
    }

    @Test
    public void testVersionOverflowThrowsPrestoException()
    {
        long overflow = (long) Integer.MAX_VALUE + 1L;
        try {
            namespaceHolder.getCachedDataset(table1Path, Optional.of(overflow));
            fail("Expected PrestoException for version exceeding Integer.MAX_VALUE");
        }
        catch (PrestoException expected) {
            assertEquals(expected.getErrorCode(), LanceErrorCode.LANCE_ERROR.toErrorCode());
            // checkArgument inside the loader raises IllegalArgumentException; it is
            // wrapped by the cache and surfaced as the cause of the PrestoException.
            assertNotNull(expected.getCause());
            assertEquals(expected.getCause().getClass(), IllegalArgumentException.class);
        }
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullTablePathThrows()
    {
        namespaceHolder.getCachedDataset(null, Optional.empty());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullVersionThrows()
    {
        namespaceHolder.getCachedDataset(table1Path, null);
    }
}
