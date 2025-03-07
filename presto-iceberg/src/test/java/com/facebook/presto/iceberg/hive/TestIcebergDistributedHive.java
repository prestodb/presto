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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.iceberg.IcebergHiveMetadata;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.CatalogMetadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestIcebergDistributedHive
        extends IcebergDistributedTestBase
{
    public TestIcebergDistributedHive()
    {
        super(HIVE, ImmutableMap.of("iceberg.hive-statistics-merge-strategy", Joiner.on(",").join(NUMBER_OF_DISTINCT_VALUES.name(), TOTAL_SIZE_IN_BYTES.name())));
    }

    @Override
    public void testNDVsAtSnapshot()
    {
        // ignore because HMS doesn't support statistics versioning
    }

    @Override
    public void testStatsByDistance()
    {
        // ignore because HMS doesn't support statistics versioning
    }

    @Override
    public void testPartShowStatsWithFilters()
    {
        // Hive doesn't support returning statistics on partitioned tables
    }

    @Override
    public void testStatisticsFileCache()
            throws Exception
    {
        // hive doesn't write Iceberg statistics files when metastore is in use,
        // so this test won't complete successfully.
    }

    @Test
    public void testManifestFileCaching()
            throws Exception
    {
        String catalogName = "iceberg_manifest_caching";
        Map<String, String> catalogProperties = new HashMap<>(this.icebergQueryRunner.getIcebergCatalogs().get("iceberg"));
        catalogProperties.put("iceberg.io.manifest.cache-enabled", "true");
        getQueryRunner().createCatalog(catalogName, "iceberg", catalogProperties);
        Session session = Session.builder(getSession())
                .setCatalog(catalogName)
                .setSchema("default")
                .build();
        assertQuerySucceeds(session, "CREATE SCHEMA IF NOT EXISTS default");
        assertQuerySucceeds(session, "CREATE TABLE test_manifest_file_cache(i int)");
        Session txnSession = Session.builder(session)
                .setTransactionId(getQueryRunner().getTransactionManager().beginTransaction(false))
                .build();
        Optional<TableHandle> handle = MetadataUtil.getOptionalTableHandle(txnSession,
                getQueryRunner().getTransactionManager(),
                QualifiedObjectName.valueOf(txnSession.getCatalog().get(), txnSession.getSchema().get(), "test_manifest_file_cache"),
                Optional.empty());
        CatalogMetadata catalogMetadata = getQueryRunner().getTransactionManager()
                .getCatalogMetadata(txnSession.getTransactionId().get(), handle.get().getConnectorId());
        Field delegate = ClassLoaderSafeConnectorMetadata.class.getDeclaredField("delegate");
        delegate.setAccessible(true);
        IcebergHiveMetadata metadata = (IcebergHiveMetadata) delegate.get(catalogMetadata.getMetadataFor(handle.get().getConnectorId()));
        ManifestFileCache manifestFileCache = metadata.getManifestFileCache();
        assertUpdate(session, "INSERT INTO test_manifest_file_cache VALUES 1, 2, 3, 4, 5", 5);
        manifestFileCache.invalidateAll();
        assertEquals(manifestFileCache.size(), 0);
        CacheStats initial = manifestFileCache.stats();
        assertQuerySucceeds(session, "SELECT count(*) from test_manifest_file_cache group by i");
        CacheStats firstQuery = manifestFileCache.stats();
        assertTrue(firstQuery.minus(initial).missCount() > 0);
        assertTrue(manifestFileCache.size() > 0);
        assertQuerySucceeds(session, "SELECT count(*) from test_manifest_file_cache group by i");
        CacheStats secondQuery = manifestFileCache.stats();
        assertEquals(secondQuery.minus(firstQuery).missCount(), 0);
        assertTrue(secondQuery.minus(firstQuery).hitCount() > 0);
        assertTrue(manifestFileCache.size() > 0);

        assertQuerySucceeds(session, "DROP TABLE test_manifest_file_cache");
        assertQuerySucceeds(session, "DROP SCHEMA default");
    }

    @Test
    public void testManifestFileCachingDisabled()
            throws Exception
    {
        String catalogName = "iceberg_no_manifest_caching";
        Map<String, String> catalogProperties = new HashMap<>(this.icebergQueryRunner.getIcebergCatalogs().get("iceberg"));
        catalogProperties.put("iceberg.io.manifest.cache-enabled", "false");
        getQueryRunner().createCatalog(catalogName, "iceberg", catalogProperties);
        Session session = Session.builder(getSession())
                .setCatalog(catalogName)
                .setSchema("default")
                .build();
        assertQuerySucceeds(session, "CREATE SCHEMA IF NOT EXISTS default");
        assertQuerySucceeds(session, "CREATE TABLE test_manifest_file_cache_disabled(i int)");
        assertUpdate(session, "INSERT INTO test_manifest_file_cache_disabled VALUES 1, 2, 3, 4, 5", 5);
        Session metadataSession = Session.builder(session)
                .setTransactionId(getQueryRunner().getTransactionManager().beginTransaction(false))
                .build();
        Optional<TableHandle> handle = MetadataUtil.getOptionalTableHandle(metadataSession,
                getQueryRunner().getTransactionManager(),
                QualifiedObjectName.valueOf(metadataSession.getCatalog().get(), metadataSession.getSchema().get(), "test_manifest_file_cache_disabled"),
                Optional.empty());
        CatalogMetadata catalogMetadata = getQueryRunner().getTransactionManager()
                .getCatalogMetadata(metadataSession.getTransactionId().get(), handle.get().getConnectorId());
        Field delegate = ClassLoaderSafeConnectorMetadata.class.getDeclaredField("delegate");
        delegate.setAccessible(true);
        IcebergHiveMetadata metadata = (IcebergHiveMetadata) delegate.get(catalogMetadata.getMetadataFor(handle.get().getConnectorId()));
        ManifestFileCache manifestFileCache = metadata.getManifestFileCache();
        assertFalse(manifestFileCache.isEnabled());
        CacheStats initial = manifestFileCache.stats();
        assertQuerySucceeds(session, "SELECT count(*) from test_manifest_file_cache_disabled group by i");
        CacheStats firstQuery = manifestFileCache.stats();
        assertEquals(firstQuery.minus(initial).hitCount(), 0);
        assertEquals(manifestFileCache.size(), 0);
        assertQuerySucceeds(session, "SELECT count(*) from test_manifest_file_cache_disabled group by i");
        CacheStats secondQuery = manifestFileCache.stats();
        assertEquals(secondQuery.minus(firstQuery).hitCount(), 0);
        assertEquals(manifestFileCache.size(), 0);

        assertQuerySucceeds(session, "DROP TABLE test_manifest_file_cache_disabled");
        assertQuerySucceeds(session, "DROP SCHEMA default");
    }

    @Override
    protected Table loadTable(String tableName)
    {
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();

        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024 * 1024),
                getQueryRunner().getDefaultSession().toConnectorSession(connectorId),
                SchemaTableName.valueOf("tpch." + tableName));
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        IcebergFileHiveMetastore fileHiveMetastore = new IcebergFileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().toString(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }
}
