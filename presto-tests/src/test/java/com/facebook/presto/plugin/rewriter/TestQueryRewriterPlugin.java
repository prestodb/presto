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

package com.facebook.presto.plugin.rewriter;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.SystemSessionProperties.IS_QUERY_REWRITER_PLUGIN_SUCCEEDED;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQueryRunner;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestQueryRewriterPlugin
{
    public static final String TARGET_SQL = "SELECT avg(l.extendedprice) AS avg_price, count(*) AS count_order FROM lineitem AS l";
    private DistributedQueryRunner queryRunner;
    private Session session;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty(SystemSessionProperties.IS_QUERY_REWRITER_PLUGIN_ENABLED, "true")
                .build();
        queryRunner = createQueryRunner();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        closeQuietly(queryRunner);
        queryRunner = null;
    }

    @Test(timeOut = 240_000)
    public void testRewriteToFixedQueryRewriter()
            throws InterruptedException
    {
        queryRunner.installPlugin(new RewriteToFixedQueryRewriterPlugin(TARGET_SQL));
        queryRunner.getCoordinator().getQueryRewriterManager().loadQueryRewriterProvider();
        queryRunner.waitForClusterToGetReady();
        String sql = "select * from customer";
        QueryId queryId = createQuery(queryRunner, session, sql);
        waitForQueryState(queryRunner, 0, queryId, ImmutableSet.of(RUNNING, FINISHED));
        QueryInfo queryInfo = queryRunner.getQueryInfo(queryId);
        assertEquals(queryInfo.getQuery(), TARGET_SQL);
        assertEquals(queryInfo.getSession().getSystemProperties().getOrDefault(IS_QUERY_REWRITER_PLUGIN_SUCCEEDED, "false"), "true");
    }

    @Test(timeOut = 240_000)
    public void testUpperCasingQueryRewriter()
            throws InterruptedException
    {
        queryRunner.installPlugin(new UpperCasingQueryRewriterPlugin());
        queryRunner.getCoordinator().getQueryRewriterManager().loadQueryRewriterProvider();
        queryRunner.waitForClusterToGetReady();
        QueryId queryId = createQuery(queryRunner, session, TARGET_SQL);
        waitForQueryState(queryRunner, 0, queryId, ImmutableSet.of(RUNNING, FINISHED));
        QueryInfo queryInfo = queryRunner.getQueryInfo(queryId);
        assertEquals(queryInfo.getQuery(), TARGET_SQL.toUpperCase());
        assertEquals(queryInfo.getSession().getSystemProperties().getOrDefault(IS_QUERY_REWRITER_PLUGIN_SUCCEEDED, "false"), "true");
    }

    @Test(timeOut = 240_000)
    public void testRewriteToFixedInvalidQueryRewriter()
            throws InterruptedException
    {
        String invalidQuery = "invalid";
        queryRunner.installPlugin(new RewriteToFixedQueryRewriterPlugin(invalidQuery));
        queryRunner.getCoordinator().getQueryRewriterManager().loadQueryRewriterProvider();
        queryRunner.waitForClusterToGetReady();
        QueryId queryId = createQuery(queryRunner, session, TARGET_SQL);
        waitForQueryState(queryRunner, 0, queryId, ImmutableSet.of(FAILED));
    }

    @Test(timeOut = 240_000)
    public void testExceptionThrowingQueryRewriterPlugin()
            throws InterruptedException
    {
        queryRunner.installPlugin(new FailingQueryRewriterPlugin());
        queryRunner.getCoordinator().getQueryRewriterManager().loadQueryRewriterProvider();
        queryRunner.waitForClusterToGetReady();
        QueryId queryId = createQuery(queryRunner, session, TARGET_SQL);
        waitForQueryState(queryRunner, 0, queryId, ImmutableSet.of(FAILED));
        // When query fails, we cannot get query info.
    }
}
