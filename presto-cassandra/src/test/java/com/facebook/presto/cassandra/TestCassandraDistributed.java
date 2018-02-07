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
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.facebook.presto.tests.FeatureSelection;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.TestedFeature.ADD_COLUMN;
import static com.facebook.presto.tests.TestedFeature.CREATE_TABLE;
import static com.facebook.presto.tests.TestedFeature.DATE_TYPE;
import static com.facebook.presto.tests.TestedFeature.DELETE;
import static com.facebook.presto.tests.TestedFeature.DROP_COLUMN;
import static com.facebook.presto.tests.TestedFeature.INSERT;
import static com.facebook.presto.tests.TestedFeature.RENAME_COLUMN;
import static com.facebook.presto.tests.TestedFeature.RENAME_TABLE;
import static com.facebook.presto.tests.TestedFeature.VIEW;

//Integrations tests fail when parallel, due to a bug or configuration error in the embedded
//cassandra instance. This problem results in either a hang in Thrift calls or broken sockets.
@Test(singleThreaded = true)
public class TestCassandraDistributed
        extends AbstractTestDistributedQueries
{
    public TestCassandraDistributed()
    {
        super(CassandraQueryRunner::createCassandraQueryRunner, FeatureSelection.allFeatures().excluding(
                ADD_COLUMN,
                DROP_COLUMN,
                RENAME_COLUMN,
                RENAME_TABLE,
                VIEW,
                INSERT,
                DELETE,
                CREATE_TABLE,
                DATE_TYPE));
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
        // Cassandra connector currently does not support create table nor insert
    }
}
