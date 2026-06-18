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

import com.facebook.presto.Session;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.SystemSessionProperties.QUERY_RETRY_LIMIT;
import static com.facebook.presto.SystemSessionProperties.RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY;
import static com.facebook.presto.SystemSessionProperties.RETRY_QUERY_WITH_HISTORY_BASED_OPTIMIZATION;
import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_STATS_FROM_FAILED_QUERIES;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestHistoryBasedRetry
{
    private ListeningExecutorService executor;

    @BeforeClass(alwaysRun = true)
    public void setUp()
    {
        executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testQueryRetryWithHBONaturalFail()
            throws Exception
    {
        int retryLimit = 3;
        int retryNum = 0;

        String sql = "SELECT o.orderkey, l.partkey, l.mapcol[o.orderkey] FROM (select orderkey, partkey, mapcol " +
                "FROM (SELECT *, map(array[1], array[2]) mapcol FROM lineitem)) l " +
                "JOIN orders o ON l.partkey = o.custkey WHERE length(comment) > 10";

        Session session = Session.builder(createSession())
                .setSystemProperty(QUERY_RETRY_LIMIT, String.valueOf(retryLimit))
                .build();

        try (QueryRunner queryRunner = createQueryRunner(session)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();

            ListenableFuture<?> future = executor.submit(() -> queryRunner.execute(session, sql));
            queryFutures.add(future);

            waitForQueryToFinish(queryFutures);

            retryNum = getRetryCount(queryRunner);
        }

        assertEquals(retryNum, 1, "Retry count should be one as the query plan has changed.");
    }

    @Test
    public void testQueryRetryWithHBOForceFail()
            throws Exception
    {
        int retryNum = 0;

        String query = "SELECT if(COUNT(*)=1,1,fail(1, 'failed')) FROM part p LEFT JOIN lineitem l ON p.partkey = l.partkey WHERE l.comment like '%a%'";

        Session session = Session.builder(createSession())
                .build();

        try (QueryRunner queryRunner = createQueryRunner(session)) {
            List<ListenableFuture<?>> queryFutures = new ArrayList<>();

            ListenableFuture<?> future = executor.submit(() -> queryRunner.execute(session, query));
            queryFutures.add(future);

            waitForQueryToFinish(queryFutures);

            retryNum = getRetryCount(queryRunner);
        }

        assertEquals(retryNum, 1, "Retry count should be one as the query plan has changed and is the max retry limit.");
    }

    private int getRetryCount(QueryRunner queryRunner)
    {
        int retryNum = 0;

        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) queryRunner;
        List<BasicQueryInfo> queryInfos = distributedQueryRunner.getCoordinator().getQueryManager().getQueries();

        for (BasicQueryInfo info : queryInfos) {
            if (info.getQuery().contains("-- retry query")) {
                retryNum++;
            }
        }
        return retryNum;
    }

    private void waitForQueryToFinish(List<ListenableFuture<?>> queryFutures)
            throws Exception
    {
        for (ListenableFuture<?> future : queryFutures) {
            try {
                future.get();
            }
            catch (ExecutionException e) {
                //it is okay to fail, we are forcing it to test the retry mechanism
            }
        }
    }

    private QueryRunner createQueryRunner(Session session)
            throws Exception
    {
        QueryRunner queryRunner = new DistributedQueryRunner(session, 1);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "3"));

        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
            {
                return ImmutableList.of(new InMemoryHistoryBasedPlanStatisticsProvider());
            }
        });
        return queryRunner;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(TRACK_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(RETRY_QUERY_WITH_HISTORY_BASED_OPTIMIZATION, "true")
                .setSystemProperty(RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY, "false")
                .setSystemProperty(TRACK_HISTORY_STATS_FROM_FAILED_QUERIES, "true")
                .build();
    }
}
