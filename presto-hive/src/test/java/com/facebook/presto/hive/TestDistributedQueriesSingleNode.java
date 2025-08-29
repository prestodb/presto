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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.tpch.TpchTable.getTables;

@Test(singleThreaded = true)
public class TestDistributedQueriesSingleNode
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap.Builder<String, String> coordinatorProperties = ImmutableMap.builder();
        coordinatorProperties.put("single-node-execution-enabled", "true");
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(
                getTables(),
                ImmutableMap.of(),
                coordinatorProperties.build(),
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
}
