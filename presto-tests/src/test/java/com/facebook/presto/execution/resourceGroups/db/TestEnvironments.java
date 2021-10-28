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
package com.facebook.presto.execution.resourceGroups.db;

import com.facebook.presto.resourceGroups.db.H2ResourceGroupsDao;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.TEST_ENVIRONMENT;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.TEST_ENVIRONMENT_2;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.adhocSession;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.createQueryRunner;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getDao;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getDbConfigUrl;

@Test(singleThreaded = true)
public class TestEnvironments
{
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";

    @Test(timeOut = 240_000)
    public void testEnvironment1()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner runner = createQueryRunner(dbConfigUrl, dao, TEST_ENVIRONMENT, ImmutableMap.of(), 1)) {
            QueryId firstQuery = createQuery(runner, adhocSession(), LONG_LASTING_QUERY);
            waitForQueryState(runner, firstQuery, RUNNING);
            QueryId secondQuery = createQuery(runner, adhocSession(), LONG_LASTING_QUERY);
            waitForQueryState(runner, secondQuery, RUNNING);
        }
    }

    @Test(timeOut = 240_000)
    public void testEnvironment2()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner runner = createQueryRunner(dbConfigUrl, dao, TEST_ENVIRONMENT_2, ImmutableMap.of(), 1)) {
            QueryId firstQuery = createQuery(runner, adhocSession(), LONG_LASTING_QUERY);
            waitForQueryState(runner, firstQuery, RUNNING);
            QueryId secondQuery = createQuery(runner, adhocSession(), LONG_LASTING_QUERY);
            // there is no queueing in TEST_ENVIRONMENT_2, so the second query should fail right away
            waitForQueryState(runner, secondQuery, FAILED);
        }
    }
}
