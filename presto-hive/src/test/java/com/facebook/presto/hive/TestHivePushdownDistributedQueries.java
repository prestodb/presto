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
package com.facebook.presto.hive;

import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHivePushdownDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(
                getTables(),
                ImmutableMap.of("experimental.pushdown-subfields-enabled", "true",
                        "experimental.pushdown-dereference-enabled", "true"),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true",
                        "hive.enable-parquet-dereference-pushdown", "true",
                        "hive.partial_aggregation_pushdown_enabled", "true",
                        "hive.partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true"),
                Optional.empty());
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Test
    public void testExplainOfCreateTableAs()
    {
        String query = "CREATE TABLE copy_orders AS SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN ", query, LOGICAL));
    }
}
