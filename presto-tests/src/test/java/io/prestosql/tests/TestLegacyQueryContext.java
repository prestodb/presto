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
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.SqlTaskManager;
import io.prestosql.execution.TestingSessionContext;
import io.prestosql.memory.LegacyQueryContext;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.execution.TestQueryRunnerUtil.waitForQueryState;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

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
        queryRunner = null;
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
