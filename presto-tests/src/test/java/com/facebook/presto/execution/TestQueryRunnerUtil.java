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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class TestQueryRunnerUtil
{
    private TestQueryRunnerUtil() {}

    public static QueryId createQuery(DistributedQueryRunner queryRunner, Session session, String sql)
    {
        return createQuery(queryRunner, 0, session, sql);
    }

    public static QueryId createQuery(DistributedQueryRunner queryRunner, int coordinator, Session session, String sql)
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator(coordinator).getDispatchManager();
        getFutureValue(dispatchManager.createQuery(session.getQueryId(), "slug", 0, new TestingSessionContext(session), sql));
        return session.getQueryId();
    }

    public static void cancelQuery(DistributedQueryRunner queryRunner, QueryId queryId)
    {
        queryRunner.getCoordinator().getDispatchManager().cancelQuery(queryId);
    }

    public static void cancelQuery(DistributedQueryRunner queryRunner, int coordinator, QueryId queryId)
    {
        queryRunner.getCoordinator(coordinator).getDispatchManager().cancelQuery(queryId);
    }

    public static void waitForQueryState(DistributedQueryRunner queryRunner, QueryId queryId, QueryState expectedQueryState)
            throws InterruptedException
    {
        waitForQueryState(queryRunner, 0, queryId, ImmutableSet.of(expectedQueryState));
    }

    public static void waitForQueryState(DistributedQueryRunner queryRunner, int coordinator, QueryId queryId, QueryState expectedQueryState)
            throws InterruptedException
    {
        waitForQueryState(queryRunner, coordinator, queryId, ImmutableSet.of(expectedQueryState));
    }

    public static void waitForQueryState(DistributedQueryRunner queryRunner, QueryId queryId, Set<QueryState> expectedQueryStates)
            throws InterruptedException
    {
        waitForQueryState(queryRunner, 0, queryId, expectedQueryStates);
    }

    public static void waitForQueryState(DistributedQueryRunner queryRunner, int coordinator, QueryId queryId, Set<QueryState> expectedQueryStates)
            throws InterruptedException
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator(coordinator).getDispatchManager();
        waitForQueryState(dispatchManager, queryId, expectedQueryStates);
    }

    public static void waitForQueryState(DispatchManager dispatchManager, QueryId queryId, Set<QueryState> expectedQueryStates)
            throws InterruptedException
    {
        do {
            // Heartbeat all the running queries, so they don't die while we're waiting
            for (BasicQueryInfo queryInfo : dispatchManager.getQueries()) {
                if (queryInfo.getState() == RUNNING) {
                    dispatchManager.getQueryInfo(queryInfo.getQueryId());
                }
            }
            MILLISECONDS.sleep(500);
        } while (!expectedQueryStates.contains(dispatchManager.getQueryInfo(queryId).getState()));
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setExtraProperties(extraProperties)
                .setNodeCount(2)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
