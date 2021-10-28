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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.type.BigintType;
import com.google.common.collect.ImmutableList;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Consumer;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestProgressMonitor
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private MockWebServer server;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        server = new MockWebServer();
        server.start();
    }

    @AfterMethod
    public void teardown()
            throws IOException
    {
        server.close();
    }

    private List<String> createResults()
    {
        List<Column> columns = ImmutableList.of(new Column("_col0", BigintType.BIGINT));
        return ImmutableList.<String>builder()
                .add(newQueryResults(null, 1, null, null, "WAITING_FOR_PREREQUISITES"))
                .add(newQueryResults(null, 2, null, null, "QUEUED"))
                .add(newQueryResults(1, 3, columns, null, "RUNNING"))
                .add(newQueryResults(1, 4, columns, null, "RUNNING"))
                .add(newQueryResults(0, 5, columns, ImmutableList.of(ImmutableList.of(253161)), "RUNNING"))
                .add(newQueryResults(null, null, columns, null, "FINISHED"))
                .build();
    }

    private String newQueryResults(Integer partialCancelId, Integer nextUriId, List<Column> responseColumns, List<List<Object>> data, String state)
    {
        String queryId = "20160128_214710_00012_rk68b";

        QueryResults queryResults = new QueryResults(
                queryId,
                server.url("/query.html?" + queryId).uri(),
                partialCancelId == null ? null : server.url(format("/v1/stage/%s.%s", queryId, partialCancelId)).uri(),
                nextUriId == null ? null : server.url(format("/v1/statement/%s/%s", queryId, nextUriId)).uri(),
                responseColumns,
                data,
                StatementStats.builder()
                        .setState(state)
                        .setWaitingForPrerequisites(state.equals("WAITING_FOR_PREREQUISITES"))
                        .setScheduled(true)
                        .build(),
                null,
                ImmutableList.of(),
                null,
                null);

        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    @Test
    public void test()
            throws SQLException
    {
        for (String result : createResults()) {
            server.enqueue(new MockResponse()
                    .addHeader(CONTENT_TYPE, "application/json")
                    .setBody(result));
        }

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
                assertEquals(queryStatsList.get(0).getState(), "WAITING_FOR_PREREQUISITES");
                assertEquals(queryStatsList.get(queryStatsList.size() - 1).getState(), "FINISHED");
            }
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:presto://%s", server.url("/").uri().getAuthority());
        return DriverManager.getConnection(url, "test", null);
    }

    private static class RecordingProgressMonitor
            implements Consumer<QueryStats>
    {
        private final ImmutableList.Builder<QueryStats> builder = ImmutableList.builder();
        private boolean finished;

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
