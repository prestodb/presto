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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    private final ConnectorSession sampledSession;

    public AbstractTestIntegrationSmokeTest(QueryRunner queryRunner, ConnectorSession sampledSession)
    {
        super(queryRunner);
        this.sampledSession = checkNotNull(sampledSession, "sampledSession is null");
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
    public void testApproximateQuerySum()
            throws Exception
    {
        assertApproximateQuery(sampledSession, "SELECT SUM(totalprice) FROM orders APPROXIMATE AT 99.999 CONFIDENCE", "SELECT 2 * SUM(totalprice) FROM orders");
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
    public void testShowSchemas()
            throws Exception
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toJdbcTypes();
        MaterializedResult expectedSchemas = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR)
                .row("tpch")
                .row("tpch_sampled")
                .build();
        assertTrue(actualSchemas.getMaterializedRows().containsAll(expectedSchemas.getMaterializedRows()));
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        MaterializedResult actualTables = computeActual("SHOW TABLES").toJdbcTypes();
        MaterializedResult expectedTables = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR)
                .row("orders")
                .build();
        assertEquals(actualTables, expectedTables);
    }

    @Test
    public void testDescribeTable()
            throws Exception
    {
        MaterializedResult actualColumns = computeActual("DESC ORDERS").toJdbcTypes();
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR, BOOLEAN, BOOLEAN, VARCHAR)
                .row("orderkey", "bigint", true, false, null)
                .row("custkey", "bigint", true, false, null)
                .row("orderstatus", "varchar", true, false, null)
                .row("totalprice", "double", true, false, null)
                .row("orderdate", "varchar", true, false, null)
                .row("orderpriority", "varchar", true, false, null)
                .row("clerk", "varchar", true, false, null)
                .row("shippriority", "bigint", true, false, null)
                .row("comment", "varchar", true, false, null)
                .build();
        assertEquals(actualColumns, expectedColumns);
    }
}
