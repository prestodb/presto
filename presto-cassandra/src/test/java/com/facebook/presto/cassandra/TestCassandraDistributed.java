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
package com.facebook.presto.cassandra;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

//Integrations tests fail when parallel, due to a bug or configuration error in the embedded
//cassandra instance. This problem results in either a hang in Thrift calls or broken sockets.

public class TestCassandraDistributed
        extends AbstractTestDistributedQueries
{
    private CassandraServer server;
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new CassandraServer();
        return CassandraQueryRunner.createCassandraQueryRunner(server);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.close();
    }
    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    public void testJoinWithLessThanOnDatesInJoinClause()
    {
        // Cassandra does not support DATE
    }

    @Override
    public void testRenameTable()
    {
        // Cassandra does not support renaming tables
    }

    @Override
    public void testAddColumn()
    {
        // Cassandra does not support adding columns
    }

    @Override
    public void testRenameColumn()
    {
        // Cassandra does not support renaming columns
    }

    @Override
    public void testDropColumn()
    {
        // Cassandra does not support dropping columns
    }

    @Override
    public void testInsert()
    {
        // TODO Cassandra connector supports inserts, but the test would fail
    }

    @Override
    public void testCreateTable()
    {
        // Cassandra connector currently does not support create table
    }

    @Override
    public void testDelete()
    {
        // Cassandra connector currently does not support delete
    }

    @Override
    public void testNonAutoCommitTransactionWithFailAndRollback()
    {
        // Ignore since Cassandra connector currently does not support create table
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testShowColumns(@Optional("PARQUET") String storageFormat)
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("custkey", "bigint", "", "", Long.valueOf(19), null, null)
                .row("orderstatus", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("totalprice", "double", "", "", Long.valueOf(53), null, null)
                .row("orderdate", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("orderpriority", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("clerk", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("shippriority", "integer", "", "", Long.valueOf(10), null, null)
                .row("comment", "varchar", "", "", null, null, Long.valueOf(2147483647))
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
    public void testWrittenStats()
    {
        // TODO Cassandra connector supports CTAS and inserts, but the test would fail
    }

    @Override
    public void testPayloadJoinApplicability()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
        // no op -- test not supported due to lack of support for array types.
    }
    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // Connector only supports writes using ctas
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // Connector only supports writes using ctas
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    public void testStringFilters()
    {
        // no op -- test not supported due to lack of support for char type.
    }

    @Override
    public void testSubfieldAccessControl()
    {
        // no op -- test not supported due to lack of support for array types.
    }

    @Override
    protected String getDateExpression(String storageFormat, String columnExpression)
    {
        return "cast(" + columnExpression + " as DATE)";
    }
}
