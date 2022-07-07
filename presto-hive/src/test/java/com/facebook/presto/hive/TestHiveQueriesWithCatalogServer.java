package com.facebook.presto.hive;

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
    public void testCatalogServerInitialization()
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        DistributedQueryRunner queryRunnerWithCatalogServer = (DistributedQueryRunner) getQueryRunnerWithCatalogServer();

        assertFalse(queryRunner.getCatalogServer().isPresent());
        assertTrue(queryRunnerWithCatalogServer.getCatalogServer().isPresent());

        assertEquals(queryRunner.getMetadata().getClass(), MetadataManager.class);
        assertEquals(queryRunnerWithCatalogServer.getMetadata().getClass(), RemoteMetadataManager.class);
    }

    @Test
    public void testTableQueryComparison()
    {
        QueryRunner queryRunner = getQueryRunner();
        QueryRunner queryRunnerWithCatalogServer = getQueryRunnerWithCatalogServer();

        String query1 = "SELECT name FROM tpch.sf1.nation";
        String query2 = "SELECT * FROM system.information_schema.schemata";

        assertEquals(queryRunner.execute(query1), queryRunnerWithCatalogServer.execute(query1));
        assertEquals(queryRunner.execute(query2), queryRunnerWithCatalogServer.execute(query2));
    }

    @Test
    public void testViewQueryComparison()
    {
        QueryRunner queryRunner = getQueryRunner();
        QueryRunner queryRunnerWithCatalogServer = getQueryRunnerWithCatalogServer();

        String view = "columns_view";
        try {
            String createView = format("CREATE VIEW %s AS SELECT * FROM system.information_schema.columns", view);
            String query = format("SELECT * FROM %s", view);

            queryRunner.execute(createView);
            queryRunnerWithCatalogServer.execute(createView);

            assertEquals(queryRunner.execute(query), queryRunnerWithCatalogServer.execute(query));
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

        String materializedView = "customer_materialized_view";
        String table = "partitioned_customer";
        try {
            String createTable = format("CREATE TABLE %s WITH (partitioned_by = ARRAY['nationkey']) AS" +
                    " SELECT custkey, name, address, nationkey FROM customer", table);
            String createMaterializedView = format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['nationkey']) AS" +
                    " SELECT name, nationkey FROM %s", materializedView, table);
            String query = format("SELECT * FROM %s", materializedView);

            queryRunner.execute(createTable);
            queryRunnerWithCatalogServer.execute(createTable);

            queryRunner.execute(createMaterializedView);
            queryRunnerWithCatalogServer.execute(createMaterializedView);

            MaterializedResult result1 = queryRunner.execute(query);
            MaterializedResult result2 = queryRunnerWithCatalogServer.execute(query);

            assertEquals(result1.getTypes(), result2.getTypes());
            assertTrue(result1.getMaterializedRows().containsAll(result2.getMaterializedRows()) && result2.getMaterializedRows().containsAll(result1.getMaterializedRows()));
            assertEquals(result1.getRowCount(), result2.getRowCount());
            assertEquals(result1.getSetSessionProperties(), result2.getSetSessionProperties());
            assertEquals(result1.getResetSessionProperties(), result2.getResetSessionProperties());
            assertEquals(result1.getUpdateType(), result2.getUpdateType());
            assertEquals(result1.getUpdateCount(), result2.getUpdateCount());
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
}
