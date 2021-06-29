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

import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.QueryLimit.Source.RESOURCE_GROUP;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_CPU_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestResourceGroupPerQueryLimitEnforcement
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().build();
        TestingPrestoServer server = queryRunner.getCoordinator();
        server.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        server.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @AfterMethod
    public void cancelAllQueriesAfterTest()
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        ImmutableList.copyOf(dispatchManager.getQueries()).forEach(queryInfo -> dispatchManager.cancelQuery(queryInfo.getQueryId()));
    }

    @Test(timeOut = 60_000L)
    public void testQueryCpuLimit()
            throws Exception
    {
        QueryId queryId = createQuery(
                queryRunner,
                testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema(TINY_SCHEMA_NAME)
                        .setSource("per_query_cpu_limit_test")
                        .build(),
                "SELECT COUNT(*) FROM lineitem");
        waitForQueryState(queryRunner, queryId, FAILED);
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        assertEquals(queryInfo.getState(), FAILED);
        assertEquals(queryInfo.getErrorCode(), EXCEEDED_CPU_LIMIT.toErrorCode());
        assertTrue(queryInfo.getFailureInfo().getMessage().contains(RESOURCE_GROUP.name()));
    }

    @Test(timeOut = 60_000L)
    public void testQueryMemoryLimit()
            throws Exception
    {
        QueryId queryId = createQuery(
                queryRunner,
                testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema(TINY_SCHEMA_NAME)
                        .setSource("per_query_memory_limit_test")
                        .build(),
                "SELECT COUNT(*) FROM lineitem");
        waitForQueryState(queryRunner, queryId, FAILED);
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        assertEquals(queryInfo.getState(), FAILED);
        assertEquals(queryInfo.getErrorCode(), EXCEEDED_GLOBAL_MEMORY_LIMIT.toErrorCode());
        assertTrue(queryInfo.getFailureInfo().getMessage().contains(RESOURCE_GROUP.name()));
    }

    @Test(timeOut = 60_000L)
    public void testQueryExecutionTimeLimit()
            throws Exception
    {
        QueryId queryId = createQuery(
                queryRunner,
                testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema(TINY_SCHEMA_NAME)
                        .setSource("per_query_execution_time_limit_test")
                        .build(),
                "SELECT COUNT(*) FROM lineitem");
        waitForQueryState(queryRunner, queryId, FAILED);
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        assertEquals(queryInfo.getState(), FAILED);
        assertEquals(queryInfo.getErrorCode(), EXCEEDED_TIME_LIMIT.toErrorCode());
        assertTrue(queryInfo.getFailureInfo().getMessage().contains(RESOURCE_GROUP.name()));
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
