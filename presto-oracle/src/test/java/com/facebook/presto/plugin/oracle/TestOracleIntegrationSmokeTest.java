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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.AfterClass;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.tpch.TpchTable.ORDERS;

public class TestOracleIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final OracleServerTester oracleServer;
    private QueryRunner queryRunner;

    protected TestOracleIntegrationSmokeTest()
            throws Exception
    {
        this.oracleServer = new OracleServerTester();
        this.queryRunner = createOracleQueryRunner(oracleServer, ORDERS);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        oracleServer.close();
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar(1)", "", "", null, null, 1L)
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", "timestamp", "", "", null, null, null)
                .row("orderpriority", "varchar(15)", "", "", null, null, 15L)
                .row("clerk", "varchar(15)", "", "", null, null, 15L)
                .row("shippriority", "bigint", "", "", 19L, null, null)
                .row("comment", "varchar(79)", "", "", null, null, 79L)
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }
}
