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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergRestManifestFileCache
        extends AbstractTestQueryFramework
{
    private static final String CACHE_MBEAN = "com.facebook.presto.iceberg:name=iceberg,type=manifestfilecache";
    private static final String READ_QUERY = "SELECT count(*) FROM iceberg.manifest_cache.t GROUP BY i";

    private File warehouseLocation;
    private TestingHttpServer restServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();

        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(restServer.getBaseUrl().toString()))
                        .put("iceberg.io.manifest.cache-enabled", "true")
                        .build())
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .build()
                .getQueryRunner();
    }

    @BeforeClass
    public void setUpTable()
    {
        assertQuerySucceeds("CREATE SCHEMA IF NOT EXISTS iceberg.manifest_cache");
        assertQuerySucceeds("CREATE TABLE iceberg.manifest_cache.t(i int)");
        assertUpdate("INSERT INTO iceberg.manifest_cache.t VALUES 1, 2, 3, 4, 5", 5);
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp()
            throws Exception
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.manifest_cache.t");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS iceberg.manifest_cache");
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testManifestFileCaching()
    {
        // Counters are cumulative and unaffected by invalidation, so compare deltas
        assertQuerySucceeds("CALL iceberg.system.invalidate_manifest_file_cache()");
        assertEquals(cacheStat("size"), 0L);

        long missesBeforeFirstRead = cacheStat("misscount");
        long hitsBeforeFirstRead = cacheStat("hitcount");

        assertQuerySucceeds(READ_QUERY);

        long missesAfterFirstRead = cacheStat("misscount");
        assertTrue(missesAfterFirstRead > missesBeforeFirstRead, "first read should miss the cache");
        assertTrue(cacheStat("size") > 0, "first read should populate the cache");

        long hitsAfterFirstRead = cacheStat("hitcount");
        assertQuerySucceeds(READ_QUERY);

        assertTrue(cacheStat("hitcount") > hitsAfterFirstRead, "second read should hit the cache");
        assertEquals(cacheStat("misscount"), missesAfterFirstRead, "second read should not miss");
        assertTrue(hitsAfterFirstRead >= hitsBeforeFirstRead);

        // The invalidation procedure targets the same singleton the REST FileIO writes to
        assertQuerySucceeds("CALL iceberg.system.invalidate_manifest_file_cache()");
        assertEquals(cacheStat("size"), 0L);

        long missesBeforeReread = cacheStat("misscount");
        assertQuerySucceeds(READ_QUERY);
        assertTrue(cacheStat("misscount") > missesBeforeReread, "read after invalidation should miss again");
    }

    private long cacheStat(String name)
    {
        return (long) computeActual(String.format("SELECT sum(\"cachestats.%s\") FROM jmx.current.\"%s\"", name, CACHE_MBEAN))
                .getOnlyValue();
    }
}
