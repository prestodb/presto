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
package com.facebook.presto.cli;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.type.BigintType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.cli.ClientOptions.OutputFormat.CSV;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;

public abstract class AbstractCliTest
{
    protected static final JsonCodec<QueryResults> QUERY_RESULTS_JSON_CODEC = jsonCodec(QueryResults.class);

    protected MockWebServer server;

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

    protected ClientSession createMockClientSession()
    {
        return new ClientSession(
                server.url("/").uri(),
                "user",
                "source",
                Optional.empty(),
                ImmutableSet.of(),
                "clientInfo",
                "catalog",
                "schema",
                "America/Los_Angeles",
                Locale.ENGLISH,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                null,
                new Duration(2, MINUTES),
                true,
                ImmutableMap.of(),
                ImmutableMap.of(),
                false);
    }

    protected QueryResults createMockQueryResults()
    {
        return new QueryResults(
                "20160128_214710_00012_rk68b",
                server.url("/query.html?20160128_214710_00012_rk68b").uri(),
                null,
                null,
                ImmutableList.of(new Column("_col0", BigintType.BIGINT)),
                ImmutableList.of(ImmutableList.of(123)),
                null,
                StatementStats.builder().setState("FINISHED").build(),
                null,
                ImmutableList.of(),
                null,
                null);
    }

    protected MockResponse createMockResponse()
    {
        return new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(QUERY_RESULTS_JSON_CODEC.toJson(createMockQueryResults()));
    }

    protected void executeQueries(List<String> queries)
    {
        QueryRunner queryRunner = createQueryRunner(createMockClientSession());
        executeQueries(queryRunner, queries);
    }

    protected void executeQueries(QueryRunner queryRunner, List<String> queries)
    {
        Console console = new Console();
        for (String query : queries) {
            console.executeCommand(queryRunner, query, CSV, false);
        }
    }

    protected static QueryRunner createQueryRunner(ClientSession clientSession, boolean insecureSsl)
    {
        return new QueryRunner(
                clientSession,
                false,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                insecureSsl,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                true,
                false,
                ImmutableList.of());
    }

    protected static QueryRunner createQueryRunner(ClientSession clientSession)
    {
        return createQueryRunner(clientSession, false);
    }

    protected static void assertHeaders(String headerName, Headers headers, Set<String> expectedSessionHeaderValues)
    {
        assertEquals(ImmutableSet.copyOf(headers.values(headerName)), expectedSessionHeaderValues);
    }
}
