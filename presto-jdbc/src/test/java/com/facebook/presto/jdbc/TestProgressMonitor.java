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
package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientTypeSignature;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestProgressMonitor
{
    private static final String SERVER_ADDRESS = "127.0.0.1:8080";
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private static final String QUERY_ID = "20160128_214710_00012_rk68b";
    private static final String INFO_URI = "http://" + SERVER_ADDRESS + "/query.html?" + QUERY_ID;
    private static final String PARTIAL_CANCEL_URI = "http://" + SERVER_ADDRESS + "/v1/stage/" + QUERY_ID + ".%d";
    private static final String NEXT_URI = "http://" + SERVER_ADDRESS + "/v1/statement/" + QUERY_ID + "/%d";
    private static final List<Column> RESPONSE_COLUMNS = ImmutableList.of(new Column("_col0", "bigint", new ClientTypeSignature("bigint", ImmutableList.of())));
    private static final List<String> RESPONSES = ImmutableList.of(
            newQueryResults(null, 1, null, null, "QUEUED"),
            newQueryResults(1, 2, RESPONSE_COLUMNS, null, "RUNNING"),
            newQueryResults(1, 3, RESPONSE_COLUMNS, null, "RUNNING"),
            newQueryResults(0, 4, RESPONSE_COLUMNS, ImmutableList.of(ImmutableList.of(253161)), "RUNNING"),
            newQueryResults(null, null, RESPONSE_COLUMNS, null, "FINISHED"));

    private static String newQueryResults(Integer partialCancelId, Integer nextUriId, List<Column> responseColumns, List<List<Object>> data, String state)
    {
        QueryResults queryResults = new QueryResults(
                QUERY_ID,
                URI.create(INFO_URI),
                partialCancelId == null ? null : URI.create(format(PARTIAL_CANCEL_URI, partialCancelId)),
                nextUriId == null ? null : URI.create(format(NEXT_URI, nextUriId)),
                responseColumns,
                data,
                new StatementStats(state, state.equals("QUEUED"), true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                null,
                null);

        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    @Test
    public void test()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                PrestoStatement prestoStatement = statement.unwrap(PrestoStatement.class);
                RecordingProgressMonitor progressMonitor = new RecordingProgressMonitor();
                prestoStatement.setProgressMonitor(progressMonitor);
                try (ResultSet rs = statement.executeQuery("bogus query for testing")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertEquals(metadata.getColumnCount(), 1);
                    assertEquals(metadata.getColumnName(1), "_col0");

                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 253161L);
                    assertEquals(rs.getLong("_col0"), 253161L);

                    assertFalse(rs.next());
                }
                prestoStatement.clearProgressMonitor();

                List<QueryStats> queryStatsList = progressMonitor.finish();
                assertGreaterThanOrEqual(queryStatsList.size(), 5); // duplicate stats is possible
                assertEquals(queryStatsList.get(0).getState(), "QUEUED");
                assertEquals(queryStatsList.get(queryStatsList.size() - 1).getState(), "FINISHED");
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        HttpClient client = new TestingHttpClient(new TestingHttpClientProcessor(RESPONSES));
        QueryExecutor testQueryExecutor = QueryExecutor.create(client);
        URI uri = URI.create(format("prestotest://%s", SERVER_ADDRESS));
        return new PrestoConnection(uri, "test", testQueryExecutor);
    }

    private static class TestingHttpClientProcessor
            implements TestingHttpClient.Processor
    {
        private final Iterator<String> responses;

        public TestingHttpClientProcessor(List<String> responses)
        {
            this.responses = ImmutableList.copyOf(requireNonNull(responses, "responses is null")).iterator();
        }

        @Override
        public synchronized Response handle(Request request)
                throws Exception
        {
            checkState(responses.hasNext(), "too many requests (ran out of test responses)");
            Response response = new TestingResponse(
                    HttpStatus.OK,
                    ImmutableListMultimap.of(HttpHeaders.CONTENT_TYPE, "application/json"),
                    responses.next().getBytes());
            return response;
        }
    }

    private static class RecordingProgressMonitor
            implements Consumer<QueryStats>
    {
        private final ImmutableList.Builder<QueryStats> builder = ImmutableList.builder();
        private boolean finished = false;

        @Override
        public synchronized void accept(QueryStats queryStats)
        {
            checkState(!finished);
            builder.add(queryStats);
        }

        public synchronized List<QueryStats> finish()
        {
            finished = true;
            return builder.build();
        }
    }
}
