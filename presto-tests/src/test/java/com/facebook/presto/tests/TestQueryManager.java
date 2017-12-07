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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.TestingSessionContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryManager
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test(timeOut = 60_000L)
    public void testFailQuery()
            throws Exception
    {
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        QueryId queryId = queryManager.createQuery(new TestingSessionContext(TEST_SESSION),
                "SELECT * FROM lineitem").getQueryId();

        // wait until query starts running
        while (true) {
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            if (queryInfo.getState().isDone()) {
                fail("unexpected query state: " + queryInfo.getState());
            }
            if (queryInfo.getState() == RUNNING) {
                break;
            }
            Thread.sleep(100);
        }

        // cancel query
        queryManager.failQuery(queryId, new PrestoException(GENERIC_INTERNAL_ERROR, "mock exception"));
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        assertEquals(queryInfo.getState(), QueryState.FAILED);
        assertEquals(queryInfo.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertNotNull(queryInfo.getFailureInfo());
        assertEquals(queryInfo.getFailureInfo().getMessage(), "mock exception");
    }
}
