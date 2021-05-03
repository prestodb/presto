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
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseOracleIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingOracleServer oracleServer;
    private final QueryRunner queryRunner;

    protected BaseOracleIntegrationSmokeTest(TestingOracleServer oracleServer)
            throws Exception
    {
        this.queryRunner = createOracleQueryRunner(oracleServer, ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION));
        this.oracleServer = new TestingOracleServer();
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

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "timestamp(3)", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   \"orderkey\" decimal(19, 0),\n" +
                        "   \"custkey\" decimal(19, 0),\n" +
                        "   \"orderstatus\" varchar(1),\n" +
                        "   \"totalprice\" double,\n" +
                        "   \"orderdate\" timestamp(3),\n" +
                        "   \"orderpriority\" varchar(15),\n" +
                        "   \"clerk\" varchar(15),\n" +
                        "   \"shippriority\" decimal(10, 0),\n" +
                        "   \"comment\" varchar(79)\n" +
                        ")");
    }
}
