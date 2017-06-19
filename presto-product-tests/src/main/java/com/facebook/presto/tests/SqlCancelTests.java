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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.teradata.tempto.AfterTestWithContext;
import com.teradata.tempto.BeforeTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.query.QueryResult;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Response;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.tests.TestGroups.CANCEL_QUERY;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class SqlCancelTests
        extends ProductTest
{
    private static final long WAIT_TIME = 10000;
    private static final long STEP = 100;

    private ExecutorService executor;
    private QueryCanceller queryCanceller;

    @Inject
    @Named("databases.presto.server_address")
    private String serverAddress;

    @BeforeTestWithContext
    public void setUp()
            throws Exception
    {
        executor = newFixedThreadPool(2);
        queryCanceller = new QueryCanceller(serverAddress);
    }

    @AfterTestWithContext
    public void cleanUp()
            throws Exception
    {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (queryCanceller != null) {
            queryCanceller.close();
        }
    }

    private class SqlExecuteCallable
            implements Callable<Void>
    {
        private String sql;

        public SqlExecuteCallable(String sql)
        {
            this.sql = sql;
        }

        public Void call()
        {
            assertThat(() -> query(sql)).failsWithMessage("Query was canceled");
            return null;
        }
    }

    private class SqlCancelCallable
            implements Callable<Response>
    {
        private String sqlStatement;

        public SqlCancelCallable(String sqlStatement)
        {
            this.sqlStatement = sqlStatement;
        }

        public Response call()
        {
            long endTime = System.currentTimeMillis() + WAIT_TIME;
            while (System.currentTimeMillis() < endTime && !Thread.currentThread().isInterrupted()) {
                QueryResult queryResult = query(format("SELECT state, query_id from system.runtime.queries" +
                        " WHERE query = '%s' ORDER BY created DESC LIMIT 1", sqlStatement));
                if (queryResult.getRowsCount() == 0) {
                    sleepUninterruptibly(STEP, TimeUnit.MILLISECONDS);
                    continue;
                }
                String queryState = (String) queryResult.row(0).get(0);
                String queryId = (String) queryResult.row(0).get(1);
                if (!queryState.equalsIgnoreCase("running")) {
                    sleepUninterruptibly(STEP, TimeUnit.MILLISECONDS);
                }
                else {
                    return queryCanceller.cancel(queryId);
                }
            }
            return null;
        }
    }

    private void doCancelTest(String sql)
            throws Exception
    {
        Future<Void> executeQueryThread = executor.submit(new SqlExecuteCallable(sql));
        Future<Response> cancelQueryThread = executor.submit(new SqlCancelCallable(sql));

        try {
            executeQueryThread.get(2, TimeUnit.MINUTES);
        }
        catch (TimeoutException e) {
            executeQueryThread.cancel(true);
            throw new RuntimeException("Query execution time limit exceeded", e);
        }

        Response response = cancelQueryThread.get();
        assertNotNull(response, "Error in cancel query thread");

        if (response.getStatusCode() != HttpStatus.OK.code() && response.getStatusCode() != HttpStatus.ACCEPTED.code() &&
                response.getStatusCode() != HttpStatus.NO_CONTENT.code()) {
            fail("Unexpected error code " + response.getStatusCode() + "; reason=" + response.getStatusMessage());
        }
    }

    @Test(groups = {CANCEL_QUERY})
    public void cancelCreateTableTest()
            throws Exception
    {
        String tableName = format("cancel_createtable_%s", randomUUID().toString().replace("-", ""));
        String sql = format("CREATE TABLE %s AS " +
                "SELECT * FROM tpch.sf1.lineitem", tableName);

        doCancelTest(sql);
        assertThat(() -> query(format("SELECT * from %s", tableName)))
                .failsWithMessage(format("Table hive.default.%s does not exist", tableName));
        query(format("DROP TABLE IF EXISTS %s", tableName));
    }

    @Test(groups = {CANCEL_QUERY})
    public void cancelInsertIntoTest()
            throws Exception
    {
        String tableName = format("cancel_insertinto_%s", randomUUID().toString().replace("-", ""));
        query(format("CREATE TABLE %s " +
                "(orderkey BIGINT, partkey BIGINT, shipinstruct VARCHAR(25)) ", tableName));
        String sql = format("INSERT INTO %s " +
                "SELECT orderkey, partkey, shipinstruct FROM tpch.sf1.lineitem", tableName);

        doCancelTest(sql);
        assertThat(query(format("SELECT * from %s", tableName))).hasNoRows();
        query(format("DROP TABLE IF EXISTS %s", tableName));
    }

    @Test(groups = {CANCEL_QUERY})
    public void cancelSelectTest()
            throws Exception
    {
        String tableName = format("cancel_select_%s", randomUUID().toString().replace("-", ""));
        String sql = format("SELECT * FROM tpch.sf1.lineitem AS %s", tableName);

        doCancelTest(sql);
    }
}
