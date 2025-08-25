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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.ITERATIVE_OPTIMIZER_TIMEOUT;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZER_USE_HISTOGRAMS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public abstract class TestIcebergDistributedQueries
        extends AbstractTestDistributedQueries
{
    private final CatalogType catalogType;
    private final Map<String, String> extraConnectorProperties;

    protected TestIcebergDistributedQueries(CatalogType catalogType, Map<String, String> extraConnectorProperties)
    {
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
        this.extraConnectorProperties = requireNonNull(extraConnectorProperties, "extraConnectorProperties is null");
    }

    protected TestIcebergDistributedQueries(CatalogType catalogType)
    {
        this(catalogType, ImmutableMap.of());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(catalogType)
                .setExtraConnectorProperties(extraConnectorProperties)
                .build().getQueryRunner();
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        // Not null columns are not yet supported by the connector
        return false;
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        // VarcharType on Iceberg cannot record its length parameter.
        // So we will get types of `varchar` for column `name` and `comment` rather than `varchar(25)` and `varchar(152)`
        //  comparing with the overridden method in parent class.
        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        // VarcharType on Iceberg cannot record its length parameter.
        // So we will get a type of `varchar` for column `name` rather than `varchar(25)`
        //  comparing with the overridden method in parent class.
        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // Catalog iceberg only supports writes using autocommit
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // Catalog iceberg only supports writes using autocommit
    }

    /**
     * Increased the optimizer timeout from 15000ms to 25000ms
     */
    @Override
    public void testLargeIn()
    {
        String longValues = range(0, 5000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        Session session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "25000ms")
                .build();
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String varcharValues = range(0, 5000)
                .mapToObj(i -> "'" + i + "'")
                .collect(joining(", "));
        assertQuery(session, "SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) IN (" + varcharValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) NOT IN (" + varcharValues + ")");

        String arrayValues = range(0, 5000)
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery(session, "SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery(session, "SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    /**
     * Increased the optimizer timeouts from 30000ms and 20000ms to 40000ms and 30000ms respectively
     */
    @Override
    public void testLargeInWithHistograms()
    {
        String longValues = range(0, 10_000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        String query = "select orderpriority, sum(totalprice) from lineitem join orders on lineitem.orderkey = orders.orderkey where orders.orderkey in (" + longValues + ") group by 1";
        Session session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "40000ms")
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                .build();
        assertQuerySucceeds(session, query);
        session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "30000ms")
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "false")
                .build();
        assertQuerySucceeds(session, query);
    }

    @Override
    public void testStringFilters()
    {
        // Type not supported for Iceberg: CHAR(10). Only test VARCHAR(10).
        // Retained exactly the latter half of the overridden method in parent class.
        assertUpdate("CREATE TABLE test_varcharn_filter (shipmode VARCHAR(10))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_varcharn_filter"));
        assertTableColumnNames("test_varcharn_filter", "shipmode");

        assertUpdate("INSERT INTO test_varcharn_filter SELECT shipmode FROM lineitem", 60175);
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR'", "VALUES (8491)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR    '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR       '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'AIR            '", "VALUES (0)");
        assertQuery("SELECT count(*) FROM test_varcharn_filter WHERE shipmode = 'NONEXIST'", "VALUES (0)");
    }

    @Test
    public void testRenameView()
    {
        skipTestUnless(supportsViews());
        assertQuerySucceeds("CREATE TABLE iceberg_test_table (_string VARCHAR, _integer INTEGER)");
        assertUpdate("CREATE VIEW test_view_to_be_renamed AS SELECT * FROM iceberg_test_table");
        assertUpdate("ALTER VIEW IF EXISTS test_view_to_be_renamed RENAME TO test_view_renamed");
        assertUpdate("CREATE VIEW test_view2_to_be_renamed AS SELECT * FROM iceberg_test_table");
        assertUpdate("ALTER VIEW test_view2_to_be_renamed RENAME TO test_view2_renamed");
        assertQuerySucceeds("SELECT * FROM test_view_renamed");
        assertQuerySucceeds("SELECT * FROM test_view2_renamed");
        assertUpdate("DROP VIEW test_view_renamed");
        assertUpdate("DROP VIEW test_view2_renamed");
        assertUpdate("DROP TABLE iceberg_test_table");
    }

    @Test
    public void testRenameViewIfNotExists()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        skipTestUnless(supportsViews());
        assertQueryFails("ALTER VIEW test_rename_view_not_exist RENAME TO test_renamed_view_not_exist",
                format("line 1:1: View '%s.%s.test_rename_view_not_exist' does not exist", catalog, schema));
        assertQuerySucceeds("ALTER VIEW IF EXISTS test_rename_view_not_exist RENAME TO test_renamed_view_not_exist");
    }
}
