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
package com.facebook.presto.connector.system;

import com.facebook.presto.Session;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.resourceGroups.ResourceGroupManagerPlugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.cancelQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQuerySystemTable
{
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";

    @Test(timeOut = 60_000)
    public void testQuerySystemTableSessionProperties()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            InternalResourceGroupManager<?> manager = queryRunner.getCoordinator().getResourceGroupManager().get();
            manager.setConfigurationManager("file", ImmutableMap.of(
                    "resource-groups.config-file", this.getClass().getClassLoader().getResource("resource_group_config_all_parameters.json").getPath()));
            QueryId queryId = createQuery(queryRunner, testSessionBuilder().setCatalog("tpch").setSchema("sf100000").setSource("dashboard").build(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, RUNNING);
            MaterializedResult result = queryRunner.execute("SELECT session_properties FROM system.runtime.queries WHERE source = 'dashboard'");
            assertTrue(((Map<String, String>) result.getOnlyValue()).isEmpty());
            cancelQuery(queryRunner, queryId);
            waitForQueryState(queryRunner, queryId, FAILED);
            Session dashBoardSession = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("sf100000")
                    .setSource("dashboard")
                    .setSystemProperty("query_priority", "12")
                    .build();
            queryId = createQuery(queryRunner, dashBoardSession, LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, RUNNING);

            result = queryRunner.execute("SELECT session_properties FROM system.runtime.queries WHERE query_id = '" + queryId.toString() + "'");
            Map<String, String> sessionProperties = (Map<String, String>) result.getOnlyValue();
            assertEquals(sessionProperties.size(), 1);
            assertEquals(sessionProperties.get("query_priority"), "12");
            cancelQuery(queryRunner, queryId);
            waitForQueryState(queryRunner, queryId, FAILED);
        }
    }
}
