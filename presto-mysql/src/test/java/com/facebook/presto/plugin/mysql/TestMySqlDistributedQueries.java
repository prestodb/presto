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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testcontainers.mysql.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestMySqlDistributedQueries
        extends AbstractTestDistributedQueries
{
    private final MySQLContainer mysqlContainer;

    public TestMySqlDistributedQueries()
    {
        this.mysqlContainer = new MySQLContainer("mysql:8.0")
                .withDatabaseName("tpch")
                .withUsername("testuser")
                .withPassword("testpass");
        this.mysqlContainer.start();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMySqlQueryRunner(mysqlContainer.getJdbcUrl(), ImmutableMap.of(), TpchTable.getTables());
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mysqlContainer.stop();
    }

    @Override
    public void testShowColumns(@Optional("PARQUET") String storageFormat)
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders").toTestTypes();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar(255)", "", "", null, null, 255L)
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", "date", "", "", null, null, null)
                .row("orderpriority", "varchar(255)", "", "", null, null, 255L)
                .row("clerk", "varchar(255)", "", "", null, null, 255L)
                .row("shippriority", "integer", "", "", 10L, null, null)
                .row("comment", "varchar(255)", "", "", null, null, 255L)
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    public void testDescribeOutput()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // this connector uses a non-canonical type for varchar columns in tpch
    }

    @Override
    public void testInsert()
    {
        // no op -- test not supported due to lack of support for array types.  See
        // TestMySqlIntegrationSmokeTest for insertion tests.
    }

    @Override
    public void testDelete()
    {
        // Delete is currently unsupported
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // JDBC connectors do not support multi-statement writes within transactions
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // JDBC connectors do not support multi-statement writes within transactions
    }

    @Override
    public void testNonAutoCommitTransactionWithFailAndRollback()
    {
        // JDBC connectors do not support multi-statement writes within transactions
    }

    @Override
    public void testPayloadJoinApplicability()
    {
        // MySQL does not support MAP type
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // MySQL does not support MAP type
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // MySQL does not support MAP type
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // MySQL does not support ROW type
    }

    @Override
    public void testStringFilters()
    {
        // MySQL maps char types to varchar(255), causing type mismatches
    }

    // MySQL specific tests should normally go in TestMySqlIntegrationSmokeTest
}
