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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpClientConfig;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.google.common.base.Stopwatch;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.query.QueryResult;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.presto.tests.TestGroups.CANCEL_QUERY;
import static com.google.common.base.Preconditions.checkState;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.fail;

public class SqlCancelTests
        extends ProductTest
{
    private ExecutorService executor;
    private QueryCanceller queryCanceller;
    private Closer closer;

    @Inject
    @Named("databases.presto.server_address")
    private String serverAddress;

    @BeforeTestWithContext
    public void setUp()
    {
        closer = Closer.create();
        executor = newSingleThreadExecutor(); // single thread is enough, it schedules the query to cancel
        closer.register(executor::shutdownNow);
        queryCanceller = closer.register(new QueryCanceller(serverAddress));
    }

    @AfterTestWithContext
    public void cleanUp()
            throws IOException
    {
        closer.close();
    }

    @Test(groups = {CANCEL_QUERY}, timeOut = 60_000L)
    public void cancelCreateTable()
            throws Exception
    {
        String tableName = "cancel_createtable_" + nanoTime();
        String sql = format("CREATE TABLE %s AS SELECT * FROM tpch.sf1.lineitem", tableName);

        runAndCancelQuery(sql);
        assertThat(() -> query("SELECT * from " + tableName))
                .failsWithMessage(format("Table hive.default.%s does not exist", tableName));
    }

    @Test(groups = {CANCEL_QUERY}, timeOut = 60_000L)
    public void cancelInsertInto()
            throws Exception
    {
        String tableName = "cancel_insertinto_" + nanoTime();
        query(format("CREATE TABLE %s (orderkey BIGINT, partkey BIGINT, shipinstruct VARCHAR(25)) ", tableName));
        String sql = format("INSERT INTO %s SELECT orderkey, partkey, shipinstruct FROM tpch.sf1.lineitem", tableName);
        runAndCancelQuery(sql);
        assertThat(query("SELECT * from " + tableName)).hasNoRows();
        query("DROP TABLE " + tableName);
    }

    @Test(groups = {CANCEL_QUERY}, timeOut = 60_000L)
    public void cancelSelect()
            throws Exception
    {
        runAndCancelQuery("SELECT * FROM tpch.sf1.lineitem AS cancel_select_" + nanoTime());
    }

    private void runAndCancelQuery(String sql)
            throws Exception
    {
        Future<?> queryExecution = executor.submit(() -> query(sql));

        cancelQuery(sql);

        try {
            queryExecution.get(30, SECONDS);
            fail("Query failure was expected");
        }
        catch (TimeoutException e) {
            queryExecution.cancel(true);
            throw e;
        }
        catch (ExecutionException expected) {
            Assertions.assertThat(expected.getCause())
                    .hasMessageEndingWith("Query was canceled");
        }
    }

    private void cancelQuery(String sql)
            throws InterruptedException
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (stopwatch.elapsed(SECONDS) < 30) {
            String findQuerySql = "SELECT query_id from system.runtime.queries WHERE query = '%s' and state = 'RUNNING' LIMIT 2";
            QueryResult queryResult = query(format(findQuerySql, sql));
            checkState(queryResult.getRowsCount() < 2, "Query is executed multiple times");
            if (queryResult.getRowsCount() == 1) {
                String queryId = (String) queryResult.row(0).get(0);
                Response response = queryCanceller.cancel(queryId);
                Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT.code());
                return;
            }
            MILLISECONDS.sleep(100L);
        }
        throw new IllegalStateException("Query did not reach running state or maybe it was too quick.");
    }

    private static class QueryCanceller
            implements Closeable
    {
        private final HttpClient httpClient;
        private final URI uri;

        QueryCanceller(String uri)
        {
            this.httpClient = new JettyHttpClient(new HttpClientConfig());
            this.uri = URI.create(requireNonNull(uri, "uri is null"));
        }

        public Response cancel(String queryId)
        {
            requireNonNull(queryId, "queryId is null");
            URI cancelUri = uriBuilderFrom(uri).appendPath("/v1/query").appendPath(queryId).build();
            Request request = prepareDelete().setUri(cancelUri).build();
            return httpClient.execute(request, new ResponseHandler<Response, RuntimeException>()
            {
                @Override
                public Response handleException(Request request, Exception exception)
                {
                    throw propagate(request, exception);
                }

                @Override
                public Response handle(Request request, Response response)
                {
                    return response;
                }
            });
        }

        @Override
        public void close()
        {
            httpClient.close();
        }
    }
}
