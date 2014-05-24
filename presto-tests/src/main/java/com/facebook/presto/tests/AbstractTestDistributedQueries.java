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
package com.facebook.presto.tests;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchMetadata;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestDistributedQueries
        extends AbstractTestSampledQueries
{
    private static final Logger log = Logger.get("TestQueries");

    private QueryRunner queryRunner;

    public abstract ConnectorSession getSampledSession();

    @Override
    protected ConnectorSession setUpQueryFramework()
            throws Exception
    {
        queryRunner = createQueryRunner();

        log.info("Loading data...");
        long startTime = System.nanoTime();
        distributeData("tpch", TpchMetadata.TINY_SCHEMA_NAME, getSession());
        distributeData("tpch_sampled", TpchMetadata.TINY_SCHEMA_NAME, getSampledSession());
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

        return getSession();
    }

    protected abstract QueryRunner createQueryRunner()
            throws Exception;

    private void distributeData(String sourceCatalog, String sourceSchema, ConnectorSession session)
            throws Exception
    {
        for (QualifiedTableName table : queryRunner.listTables(session, sourceCatalog, sourceSchema)) {
            if (table.getTableName().equalsIgnoreCase("dual")) {
                continue;
            }
            log.info("Running import for %s", table.getTableName());
            @Language("SQL") String sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getTableName(), table);
            long rows = checkType(queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0), Long.class, "rows");
            log.info("Imported %s rows for %s", rows, table.getTableName());
        }
    }

    @Override
    protected int getNodeCount()
    {
        return queryRunner.getNodeCount();
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        return queryRunner.execute(getSession(), sql);
    }

    @Override
    protected MaterializedResult computeActualSampled(@Language("SQL") String sql)
    {
        return queryRunner.execute(getSampledSession(), sql);
    }

    private void assertCreateTable(String table, @Language("SQL") String query, @Language("SQL") String rowCountQuery)
            throws Exception
    {
        assertCreateTable(table, query, query, rowCountQuery);
    }

    private void assertCreateTable(String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
            throws Exception
    {
        assertQuery("CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery("SELECT * FROM " + table, expectedQuery);
        assertQueryTrue("DROP TABLE " + table);

        assertFalse(queryRunner.tableExists(getSession(), table));
    }

    @Override
    protected void tearDownQueryFramework()
            throws Exception
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testCreateSampledTableAsSelectLimit()
            throws Exception
    {
        assertCreateTable(
                "test_limit_sampled",
                "SELECT orderkey FROM tpch_sampled.tiny.orders ORDER BY orderkey LIMIT 10",
                "SELECT orderkey FROM (SELECT orderkey FROM orders) UNION ALL (SELECT orderkey FROM orders) ORDER BY orderkey LIMIT 10",
                "SELECT 10");
    }

    @Test
    public void testCreateTableAsSelect()
            throws Exception
    {
        assertCreateTable(
                "test_simple",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");
    }

    @Test
    public void testCreateTableAsSelectGroupBy()
            throws Exception
    {
        assertCreateTable(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");
    }

    @Test
    public void testCreateTableAsSelectJoin()
            throws Exception
    {
        assertCreateTable(
                "test_join",
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");
    }

    @Test
    public void testCreateTableAsSelectLimit()
            throws Exception
    {
        assertCreateTable(
                "test_limit",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*statement is too large.*")
    public void testLargeQueryFailure()
            throws Exception
    {
        assertQuery("SELECT " + Joiner.on(" AND ").join(nCopies(1000, "1 = 1")), "SELECT true");
    }

    @Test
    public void testLargeQuerySuccess()
            throws Exception
    {
        assertQuery("SELECT " + Joiner.on(" AND ").join(nCopies(500, "1 = 1")), "SELECT true");
    }

    @Test
    public void testShowSchemasFromOther()
            throws Exception
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS FROM tpch"));
        ImmutableSet<String> schemaNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertTrue(schemaNames.containsAll(ImmutableSet.of(INFORMATION_SCHEMA, "sys", "tiny")));
    }

    @Test
    public void testTableSampleSystem()
            throws Exception
    {
        int total = computeActual("SELECT orderkey FROM orders").getMaterializedRows().size();

        boolean sampleSizeFound = false;
        for (int i = 0; i < 100; i++) {
            int sampleSize = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE SYSTEM (50)").getMaterializedRows().size();
            if (sampleSize > 0 && sampleSize < total) {
                sampleSizeFound = true;
                break;
            }
        }
        assertTrue(sampleSizeFound, "Table sample returned unexpected number of rows");
    }

    @Test
    public void testTableSampleSystemBoundaryValues()
            throws Exception
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (0)");
        MaterializedResult all = computeActual("SELECT orderkey FROM orders");

        assertTrue(all.getMaterializedRows().containsAll(fullSample.getMaterializedRows()));
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }
}
