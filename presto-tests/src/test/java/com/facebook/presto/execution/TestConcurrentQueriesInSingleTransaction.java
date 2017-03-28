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
import com.facebook.presto.resourceGroups.ResourceGroupManagerPlugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.QueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.QueryRunnerUtil.createQueryRunner;
import static com.facebook.presto.execution.QueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CONCURRENT_QUERY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestConcurrentQueriesInSingleTransaction
{
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";

    @Test(timeOut = 60_000)
    public void testConcurrentQueryInSingleTransaction()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of())) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());

            Session clientSession = newTransactionSession();

            // start transaction
            QueryId startTxnQuery = createQuery(queryRunner, clientSession, "START TRANSACTION");

            // wait until the transaction is registered
            waitForQueryState(queryRunner, startTxnQuery, FINISHED);

            assertEquals(queryRunner.getTransactionManager().getAllTransactionInfos().size(), 1);
            TransactionId txnId = queryRunner.getTransactionManager().getAllTransactionInfos().get(0).getTransactionId();

            // submit first "cli" query
            QueryId firstCliQuery = createQuery(queryRunner, clientSession, txnId, LONG_LASTING_QUERY);

            // wait for the first query to start
            waitForQueryState(queryRunner, firstCliQuery, RUNNING);

            // submit second "cli" query without waiting
            QueryId secondCliQuery = createQuery(queryRunner, clientSession, txnId, LONG_LASTING_QUERY);

            // the second query should failed
            QueryInfo failedQueryInfo = queryRunner.getCoordinator().getQueryManager().getQueryInfo(secondCliQuery);
            assertEquals(failedQueryInfo.getErrorType(), USER_ERROR);
            assertEquals(failedQueryInfo.getErrorCode(), INVALID_CONCURRENT_QUERY.toErrorCode());

            // Cancel the first query
            QueryRunnerUtil.cancelQuery(queryRunner, firstCliQuery);
            QueryRunnerUtil.waitForQueryState(queryRunner, firstCliQuery, FAILED);
        }
    }

    private static Session newTransactionSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("cli")
                .setClientTransactionSupport()
                .build();
    }
}
