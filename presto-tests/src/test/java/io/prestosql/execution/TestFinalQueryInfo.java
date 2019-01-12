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
package io.prestosql.execution;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.client.ClientSession;
import io.prestosql.client.StatementClient;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.QueryId;
import io.prestosql.tests.DistributedQueryRunner;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;

import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.client.StatementClientFactory.newStatementClient;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertTrue;

public class TestFinalQueryInfo
{
    @Test(timeOut = 240_000)
    public void testFinalQueryInfoSetOnAbort()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TEST_SESSION)) {
            QueryId queryId = startQuery("SELECT COUNT(*) FROM tpch.sf1000.lineitem", queryRunner);
            SettableFuture<QueryInfo> finalQueryInfoFuture = SettableFuture.create();
            queryRunner.getCoordinator().addFinalQueryInfoListener(queryId, finalQueryInfoFuture::set);

            // wait 1s then kill query
            Thread.sleep(1_000);
            queryRunner.getCoordinator().getQueryManager().cancelQuery(queryId);

            // wait for final query info
            QueryInfo finalQueryInfo = tryGetFutureValue(finalQueryInfoFuture, 10, SECONDS)
                    .orElseThrow(() -> new AssertionError("Final query info never set"));
            assertTrue(finalQueryInfo.isFinalQueryInfo());
        }
    }

    private static QueryId startQuery(String sql, DistributedQueryRunner queryRunner)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            ClientSession clientSession = new ClientSession(
                    queryRunner.getCoordinator().getBaseUrl(),
                    "user",
                    "source",
                    Optional.empty(),
                    ImmutableSet.of(),
                    null,
                    null,
                    null,
                    null,
                    "America/Los_Angeles",
                    Locale.ENGLISH,
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    null,
                    new Duration(2, MINUTES));

            // start query
            StatementClient client = newStatementClient(httpClient, clientSession, sql);

            // wait for query to be fully scheduled
            while (client.isRunning() && !client.currentStatusInfo().getStats().isScheduled()) {
                client.advance();
            }

            return new QueryId(client.currentStatusInfo().getId());
        }
        finally {
            // close the client since, query is not managed by the client protocol
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }

    public static DistributedQueryRunner createQueryRunner(Session session)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
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
