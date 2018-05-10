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

import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.TestingSessionContext;
import com.facebook.presto.memory.LegacyQueryContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.airlift.testing.Assertions.assertInstanceOf;

@Test(singleThreaded = true)
public class TestLegacyQueryContext
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().setExtraProperties(ImmutableMap.of("deprecated.legacy-system-pool-enabled", "true")).build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test(timeOut = 60_000L)
    public void testLegacyQueryContext()
            throws Exception
    {
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();

        QueryId queryId = queryManager.createQueryId();
        queryManager.createQuery(
                queryId,
                new TestingSessionContext(TEST_SESSION),
                "SELECT * FROM lineitem")
                .get();

        waitForQueryState(queryRunner, queryId, RUNNING);

        // cancel query
        queryManager.failQuery(queryId, new PrestoException(GENERIC_INTERNAL_ERROR, "mock exception"));

        // assert that LegacyQueryContext is used instead of the DefaultQueryContext
        SqlTaskManager taskManager = (SqlTaskManager) queryRunner.getServers().get(0).getTaskManager();
        assertInstanceOf(taskManager.getQueryContext(queryId), LegacyQueryContext.class);
    }
}
