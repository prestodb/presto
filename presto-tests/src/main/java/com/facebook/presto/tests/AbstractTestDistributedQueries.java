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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestDistributedQueries
        extends AbstractTestApproximateQueries
{
    protected AbstractTestDistributedQueries(QueryRunner queryRunner, ConnectorSession defaultSampledSession)
    {
        super(queryRunner, defaultSampledSession);
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

    @Test
    public void testView()
            throws Exception
    {
        @Language("SQL") String query = "SELECT orderkey, orderstatus, totalprice / 2 half FROM orders";

        assertQueryTrue("CREATE VIEW test_view AS SELECT 123 x");
        assertQueryTrue("CREATE OR REPLACE VIEW test_view AS " + query);

        assertQuery("SELECT * FROM test_view", query);

        assertQuery(
                "SELECT * FROM test_view a JOIN test_view b on a.orderkey = b.orderkey",
                format("SELECT * FROM (%s) a JOIN (%s) b ON a.orderkey = b.orderkey", query, query));

        assertQuery("WITH orders AS (SELECT * FROM orders LIMIT 0) SELECT * FROM test_view", query);

        String name = format("%s.%s.test_view", getSession().getCatalog(), getSession().getSchema());
        assertQuery("SELECT * FROM " + name, query);

        assertQueryTrue("DROP VIEW test_view");
    }

    @Test
    public void testViewMetadata()
            throws Exception
    {
        @Language("SQL") String query = "SELECT 123 x, 'foo' y";
        assertQueryTrue("CREATE VIEW meta_test_view AS " + query);

        // test INFORMATION_SCHEMA.TABLES
        MaterializedResult actual = computeActual(format(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '%s'",
                getSession().getSchema()));

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("customer", "BASE TABLE")
                .row("lineitem", "BASE TABLE")
                .row("meta_test_view", "VIEW")
                .row("nation", "BASE TABLE")
                .row("orders", "BASE TABLE")
                .row("part", "BASE TABLE")
                .row("partsupp", "BASE TABLE")
                .row("region", "BASE TABLE")
                .row("supplier", "BASE TABLE")
                .build();

        assertContains(actual, expected);

        // test SHOW TABLES
        actual = computeActual("SHOW TABLES");

        MaterializedResult.Builder builder = resultBuilder(getSession(), actual.getTypes());
        for (MaterializedRow row : expected.getMaterializedRows()) {
            builder.row(row.getField(0));
        }
        expected = builder.build();

        assertContains(actual, expected);

        // test INFORMATION_SCHEMA.VIEWS
        actual = computeActual(format(
                "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '%s'",
                getSession().getSchema()));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row("meta_test_view", formatSql(new SqlParser().createStatement(query)))
                .build();

        assertContains(actual, expected);

        // test SHOW COLUMNS
        actual = computeActual("SHOW COLUMNS FROM meta_test_view");

        expected = resultBuilder(getSession(), VARCHAR, VARCHAR, BOOLEAN, BOOLEAN)
                .row("x", "bigint", true, false)
                .row("y", "varchar", true, false)
                .build();

        assertEquals(actual, expected);

        assertQueryTrue("DROP VIEW meta_test_view");
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
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM tpch");
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

    private static void assertContains(MaterializedResult actual, MaterializedResult expected)
    {
        for (MaterializedRow row : expected.getMaterializedRows()) {
            if (!actual.getMaterializedRows().contains(row)) {
                fail(format("expected row missing: %s%nActual:%n      %s%nExpected:%n      %s", row, actual, expected));
            }
        }
    }
}
