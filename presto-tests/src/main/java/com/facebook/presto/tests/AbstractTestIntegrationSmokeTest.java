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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    protected AbstractTestIntegrationSmokeTest(QueryRunner queryRunner)
    {
        super(queryRunner);
    }

    @Test
    public void testAggregateSingleColumn()
            throws Exception
    {
        assertQuery("SELECT SUM(orderkey) FROM ORDERS");
        assertQuery("SELECT SUM(totalprice) FROM ORDERS");
        assertQuery("SELECT MAX(comment) FROM ORDERS");
    }

    @Test
    public void testColumnsInReverseOrder()
            throws Exception
    {
        assertQuery("SELECT shippriority, clerk, totalprice FROM ORDERS");
    }

    @Test
    public void testCountAll()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS");
    }

    @Test
    public void testExactPredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey = 10");
    }

    @Test
    public void testInListPredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey IN (10, 11, 20, 21)");
    }

    @Test
    public void testIsNullPredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey = 10 OR orderkey IS NULL");
    }

    @Test
    public void testMultipleRangesPredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey BETWEEN 10 AND 50 or orderkey BETWEEN 100 AND 150");
    }

    @Test
    public void testRangePredicate()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    public void testSelectAll()
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS");
    }

    @Test
    public void testShowSchemas()
            throws Exception
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toJdbcTypes();

        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR)
                .row("tpch");

        assertContains(actualSchemas, resultBuilder.build());
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        MaterializedResult actualTables = computeActual("SHOW TABLES").toJdbcTypes();
        MaterializedResult expectedTables = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR)
                .row("orders")
                .build();
        assertContains(actualTables, expectedTables);
    }

    @Test
    public void testDescribeTable()
            throws Exception
    {
        MaterializedResult actualColumns = computeActual("DESC ORDERS").toJdbcTypes();

        // some connectors don't support dates, and some do not support parametrized varchars, so we check multiple options
        List<MaterializedResult> expectedColumnsPossibilities = ImmutableList.of(
                getExpectedTableDescription(true, true),
                getExpectedTableDescription(true, false),
                getExpectedTableDescription(false, true),
                getExpectedTableDescription(false, false)
        );
        assertTrue(expectedColumnsPossibilities.contains(actualColumns), String.format("%s not in %s", actualColumns, expectedColumnsPossibilities));
    }

    private MaterializedResult getExpectedTableDescription(boolean dateSupported, boolean parametrizedVarchar)
    {
        String orderDateType;
        if (dateSupported) {
            orderDateType = "date";
        }
        else {
            orderDateType = "varchar";
        }
        if (parametrizedVarchar) {
            return MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR, VARCHAR)
                    .row("orderkey", "bigint", "")
                    .row("custkey", "bigint", "")
                    .row("orderstatus", "varchar", "")
                    .row("totalprice", "double", "")
                    .row("orderdate", orderDateType, "")
                    .row("orderpriority", "varchar", "")
                    .row("clerk", "varchar", "")
                    .row("shippriority", "integer", "")
                    .row("comment", "varchar", "")
                    .build();
        }
        else {
            return MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR, VARCHAR)
                    .row("orderkey", "bigint", "")
                    .row("custkey", "bigint", "")
                    .row("orderstatus", "varchar(1)", "")
                    .row("totalprice", "double", "")
                    .row("orderdate", orderDateType, "")
                    .row("orderpriority", "varchar(15)", "")
                    .row("clerk", "varchar(15)", "")
                    .row("shippriority", "integer", "")
                    .row("comment", "varchar(79)", "")
                    .build();
        }
    }
}
