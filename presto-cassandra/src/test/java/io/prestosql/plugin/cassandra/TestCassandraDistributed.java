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
package io.prestosql.plugin.cassandra;

import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestDistributedQueries;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;

//Integrations tests fail when parallel, due to a bug or configuration error in the embedded
//cassandra instance. This problem results in either a hang in Thrift calls or broken sockets.
@Test(singleThreaded = true)
public class TestCassandraDistributed
        extends AbstractTestDistributedQueries
{
    public TestCassandraDistributed()
    {
        super(CassandraQueryRunner::createCassandraQueryRunner);
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
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
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "varchar", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
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
}
