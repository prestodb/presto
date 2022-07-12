/*
 * Copyright (C) 2013 Facebook, Inc.
 *
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
package com.facebook.presto.hive;

import com.facebook.presto.catalogserver.CatalogServerCacheStats;
import com.facebook.presto.catalogserver.RemoteMetadataManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.tpch.TpchTable.getTables;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveQueriesWithCatalogServer
{
    private QueryRunner queryRunner;
    private QueryRunner queryRunnerWithCatalogServer;

    @BeforeClass
    public void init()
            throws Exception
    {
        queryRunner = HiveQueryRunner.createQueryRunner(false, getTables());
        queryRunnerWithCatalogServer = HiveQueryRunner.createQueryRunner(true, getTables());
    }

    @Test
    public void testCatalogServerInitializationCacheStats()
    {
        CatalogServerCacheStats cacheStats = getMetadataManager().getCacheStats();

        // Assert the hit/miss count stats upon query runner initialization
        assertEquals(cacheStats.getSchemaExistsCacheStats().getCacheHitCount(), 0);
        assertEquals(cacheStats.getCatalogExistsCacheStats().getCacheHitCount(), 0);
        assertEquals(cacheStats.getListSchemaNamesCacheStats().getCacheHitCount(), 0);
        assertEquals(cacheStats.getGetTableHandleCacheStats().getCacheHitCount(), 8);
        assertEquals(cacheStats.getListTablesCacheStats().getCacheHitCount(), 0);
        assertEquals(cacheStats.getListViewsCacheStats().getCacheHitCount(), 0);
        assertEquals(cacheStats.getGetViewsCacheStats().getCacheHitCount(), 0);
        assertEquals(cacheStats.getGetViewCacheStats().getCacheHitCount(), 8);
        assertEquals(cacheStats.getGetMaterializedViewCacheStats().getCacheHitCount(), 8);
        assertEquals(cacheStats.getGetReferencedMaterializedViewsCacheStats().getCacheHitCount(), 0);

        assertEquals(cacheStats.getCatalogExistsCacheStats().getCacheMissCount(), 0);
        assertEquals(cacheStats.getSchemaExistsCacheStats().getCacheMissCount(), 0);
        assertEquals(cacheStats.getListSchemaNamesCacheStats().getCacheMissCount(), 0);
        assertEquals(cacheStats.getGetTableHandleCacheStats().getCacheMissCount(), 24);
        assertEquals(cacheStats.getListTablesCacheStats().getCacheMissCount(), 0);
        assertEquals(cacheStats.getListViewsCacheStats().getCacheMissCount(), 0);
        assertEquals(cacheStats.getGetViewsCacheStats().getCacheMissCount(), 0);
        assertEquals(cacheStats.getGetViewCacheStats().getCacheMissCount(), 8);
        assertEquals(cacheStats.getGetMaterializedViewCacheStats().getCacheMissCount(), 8);
        assertEquals(cacheStats.getGetReferencedMaterializedViewsCacheStats().getCacheMissCount(), 0);
    }

    @Test
    public void testCatalogServerConfiguration()
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        DistributedQueryRunner queryRunnerWithCatalogServer = (DistributedQueryRunner) getQueryRunnerWithCatalogServer();

        // Assert catalog server node creation
        assertFalse(queryRunner.getCatalogServer().isPresent());
        assertTrue(queryRunnerWithCatalogServer.getCatalogServer().isPresent());

        // Assert correct metadata binding
        assertEquals(queryRunner.getMetadata().getClass(), MetadataManager.class);
        assertEquals(queryRunnerWithCatalogServer.getMetadata().getClass(), RemoteMetadataManager.class);
    }

    @Test
    public void testTableQueryComparison()
    {
        QueryRunner queryRunner = getQueryRunner();
        QueryRunner queryRunnerWithCatalogServer = getQueryRunnerWithCatalogServer();
        RemoteMetadataManager metadataManager = getMetadataManager();
        metadataManager.refreshCache();

        String query1 = "SELECT name FROM tpch.sf1.nation";
        String query2 = "SELECT * FROM system.information_schema.schemata";

        // Assert query equality and cache miss for first run
        assertEquals(queryRunner.execute(query1), queryRunnerWithCatalogServer.execute(query1));
        assertEquals(queryRunner.execute(query2), queryRunnerWithCatalogServer.execute(query2));

        CatalogServerCacheStats cacheStats = metadataManager.getCacheStats();

        assertEquals(cacheStats.getGetTableHandleCacheStats().getCacheMissCount(), 2);
        assertEquals(cacheStats.getGetViewCacheStats().getCacheMissCount(), 2);
        assertEquals(cacheStats.getGetMaterializedViewCacheStats().getCacheMissCount(), 2);

        // Assert query equality and cache hit for duplicate run
        assertEquals(queryRunner.execute(query1), queryRunnerWithCatalogServer.execute(query1));
        assertEquals(queryRunner.execute(query2), queryRunnerWithCatalogServer.execute(query2));

        cacheStats = metadataManager.getCacheStats();

        assertEquals(cacheStats.getGetTableHandleCacheStats().getCacheHitCount(), 2);
        assertEquals(cacheStats.getGetViewCacheStats().getCacheHitCount(), 2);
        assertEquals(cacheStats.getGetMaterializedViewCacheStats().getCacheHitCount(), 2);
    }

    @Test
    public void testViewQueryComparison()
    {
        QueryRunner queryRunner = getQueryRunner();
        QueryRunner queryRunnerWithCatalogServer = getQueryRunnerWithCatalogServer();
        RemoteMetadataManager metadataManager = getMetadataManager();
        metadataManager.refreshCache();

        String view = "columns_view";
        try {
            String createView = format("CREATE VIEW %s AS SELECT * FROM system.information_schema.columns", view);
            String query = format("SELECT * FROM %s", view);

            // Create view from existing table
            queryRunner.execute(createView);
            queryRunnerWithCatalogServer.execute(createView);

            // Assert query equality and cache miss for first run
            assertEquals(queryRunner.execute(query), queryRunnerWithCatalogServer.execute(query));

            CatalogServerCacheStats cacheStats = metadataManager.getCacheStats();

            assertEquals(cacheStats.getGetTableHandleCacheStats().getCacheMissCount(), 1);
            assertEquals(cacheStats.getGetViewCacheStats().getCacheMissCount(), 2);
            assertEquals(cacheStats.getGetMaterializedViewCacheStats().getCacheMissCount(), 1);

            // Assert query equality and cache hit for duplicate run
            assertEquals(queryRunner.execute(query), queryRunnerWithCatalogServer.execute(query));

            cacheStats = metadataManager.getCacheStats();

            assertEquals(cacheStats.getGetTableHandleCacheStats().getCacheHitCount(), 1);
            assertEquals(cacheStats.getGetViewCacheStats().getCacheHitCount(), 2);
            assertEquals(cacheStats.getGetMaterializedViewCacheStats().getCacheHitCount(), 1);
        }
        finally {
            queryRunner.execute(format("DROP VIEW IF EXISTS %s", view));
            queryRunnerWithCatalogServer.execute(format("DROP VIEW IF EXISTS %s", view));
        }
    }

    @Test
    public void testMaterializedViewQueryComparison()
    {
        QueryRunner queryRunner = getQueryRunner();
        QueryRunner queryRunnerWithCatalogServer = getQueryRunnerWithCatalogServer();
        RemoteMetadataManager metadataManager = getMetadataManager();
        metadataManager.refreshCache();

        String materializedView = "customer_materialized_view";
        String table = "partitioned_customer";
        try {
            String createTable = format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) AS" +
                    " SELECT custkey, name, address, nationkey FROM customer", table);
            String createMaterializedView = format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey']) AS" +
                    " SELECT name, nationkey FROM %s", materializedView, table);
            String query = format("SELECT * FROM %s", materializedView);

            // Create table for materialized view
            queryRunner.execute(createTable);
            queryRunnerWithCatalogServer.execute(createTable);

            queryRunner.execute(createMaterializedView);
            queryRunnerWithCatalogServer.execute(createMaterializedView);

            // Assert query equality and cache miss for first run
            MaterializedResult result1 = queryRunner.execute(query);
            MaterializedResult result2 = queryRunnerWithCatalogServer.execute(query);

            assertEquals(result1.getTypes(), result2.getTypes());
            assertTrue(result1.getMaterializedRows().containsAll(result2.getMaterializedRows()) && result2.getMaterializedRows().containsAll(result1.getMaterializedRows()));
            assertEquals(result1.getRowCount(), result2.getRowCount());
            assertEquals(result1.getSetSessionProperties(), result2.getSetSessionProperties());
            assertEquals(result1.getResetSessionProperties(), result2.getResetSessionProperties());
            assertEquals(result1.getUpdateType(), result2.getUpdateType());
            assertEquals(result1.getUpdateCount(), result2.getUpdateCount());

            CatalogServerCacheStats cacheStats = metadataManager.getCacheStats();

            assertEquals(cacheStats.getGetTableHandleCacheStats().getCacheMissCount(), 2);
            assertEquals(cacheStats.getGetViewCacheStats().getCacheMissCount(), 3);
            assertEquals(cacheStats.getGetMaterializedViewCacheStats().getCacheMissCount(), 3);

            // Assert query equality and cache hit for duplicate run
            result1 = queryRunner.execute(query);
            result2 = queryRunnerWithCatalogServer.execute(query);

            assertEquals(result1.getTypes(), result2.getTypes());
            assertTrue(result1.getMaterializedRows().containsAll(result2.getMaterializedRows()) && result2.getMaterializedRows().containsAll(result1.getMaterializedRows()));
            assertEquals(result1.getRowCount(), result2.getRowCount());
            assertEquals(result1.getSetSessionProperties(), result2.getSetSessionProperties());
            assertEquals(result1.getResetSessionProperties(), result2.getResetSessionProperties());
            assertEquals(result1.getUpdateType(), result2.getUpdateType());
            assertEquals(result1.getUpdateCount(), result2.getUpdateCount());

            cacheStats = metadataManager.getCacheStats();

            assertEquals(cacheStats.getGetTableHandleCacheStats().getCacheHitCount(), 2);
            assertEquals(cacheStats.getGetViewCacheStats().getCacheHitCount(), 2);
            assertEquals(cacheStats.getGetMaterializedViewCacheStats().getCacheHitCount(), 2);
        }
        finally {
            queryRunner.execute(format("DROP MATERIALIZED VIEW IF EXISTS %s", materializedView));
            queryRunnerWithCatalogServer.execute(format("DROP MATERIALIZED VIEW IF EXISTS %s", materializedView));

            queryRunner.execute(format("DROP TABLE IF EXISTS %s", table));
            queryRunnerWithCatalogServer.execute(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    private QueryRunner getQueryRunner()
    {
        checkState(queryRunner != null, "queryRunner not set");
        return queryRunner;
    }

    private QueryRunner getQueryRunnerWithCatalogServer()
    {
        checkState(queryRunnerWithCatalogServer != null, "queryRunnerWithCatalogServer not set");
        return queryRunnerWithCatalogServer;
    }

    private RemoteMetadataManager getMetadataManager()
    {
        QueryRunner queryRunnerWithCatalogServer = getQueryRunnerWithCatalogServer();
        return (RemoteMetadataManager) queryRunnerWithCatalogServer.getMetadata();
    }
}
