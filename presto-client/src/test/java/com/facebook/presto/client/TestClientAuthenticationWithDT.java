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
package com.facebook.presto.client;

import com.facebook.airlift.json.JsonCodec;
import com.google.common.collect.ImmutableList;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.OkHttpUtil.setupSsl;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DELEGATION_TOKEN;
import static com.facebook.presto.client.TestMockWebServerFactory.MockWebServerInfo;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestClientAuthenticationWithDT
{
    private static final String VALID_DELEGATION_TOKEN = "This is valid token!";
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private MockWebServerInfo serverInfo;

    @BeforeTest
    public void setup()
            throws Exception
    {
        serverInfo = TestMockWebServerFactory.getMockWebServerWithSSL();
        serverInfo.server.start();
        setupDispatcher();
    }

    @Test
    public void testValidDelegationToken()
            throws Exception
    {
        OkHttpClient client = setupHttpClient();
        Call callFirst = client.newCall(new Request.Builder().post(RequestBody.create(JSON, "test query")).header(PRESTO_DELEGATION_TOKEN, VALID_DELEGATION_TOKEN).url(serverInfo.server.url("/v1/statement")).build());
        Response responseFirst = callFirst.execute();
        QueryResults first = QUERY_RESULTS_CODEC.fromJson(responseFirst.body().string());
        assertEquals(200, responseFirst.code());
        assertEquals("20220211_214710_00012_rk69b", first.getId());
        assertEquals("/v1/statement/20220211_214710_00012_rk69b/1", first.getNextUri().getPath());

        Call callSecond = client.newCall(new Request.Builder().header(PRESTO_DELEGATION_TOKEN, VALID_DELEGATION_TOKEN).url(serverInfo.server.url(first.getNextUri().getPath())).build());
        Response responseSecond = callSecond.execute();
        QueryResults second = QUERY_RESULTS_CODEC.fromJson(responseSecond.body().string());
        assertEquals("20220211_214710_00012_rk69b", second.getId());
        assertEquals("/v1/statement/20220211_214710_00012_rk69b/2", second.getNextUri().getPath());
        assertEquals(100L, second.getData().iterator().next().iterator().next());

        Call callThird = client.newCall(new Request.Builder().header(PRESTO_DELEGATION_TOKEN, VALID_DELEGATION_TOKEN).url(serverInfo.server.url(second.getNextUri().getPath())).build());
        Response responseThird = callThird.execute();
        QueryResults third = QUERY_RESULTS_CODEC.fromJson(responseThird.body().string());
        assertEquals("20220211_214710_00012_rk69b", third.getId());
        assertNull(third.getNextUri());
    }

    @Test
    public void testInValidDelegationToken()
            throws Exception
    {
        OkHttpClient client = setupHttpClient();
        Call call = client.newCall(new Request.Builder().header(PRESTO_DELEGATION_TOKEN, "Wrong Token").url(serverInfo.server.url("/v1/info")).build());
        Response response = call.execute();
        assertEquals(401, response.code());
    }

    @AfterTest
    public void teardown()
            throws IOException
    {
        serverInfo.server.close();
    }

    private OkHttpClient setupHttpClient()
    {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient().newBuilder();

        setupSsl(httpClientBuilder, Optional.empty(), Optional.empty(), Optional.of(serverInfo.trustStorePath), Optional.of(serverInfo.trustStorePassword));
        OkHttpClient httpClient = httpClientBuilder.build();

        OkHttpClient.Builder builder = httpClient.newBuilder();
        return builder.build();
    }

    private void setupDispatcher()
    {
        QueueDispatcherWithAuth dispatcherWithAuth = new QueueDispatcherWithAuth();
        for (String response : createResults()) {
            dispatcherWithAuth.enqueueResponse(new MockResponse()
                    .addHeader(CONTENT_TYPE, "application/json")
                    .setBody(response));
        }
        serverInfo.server.setDispatcher(dispatcherWithAuth);
    }

    private static class QueueDispatcherWithAuth
            extends QueueDispatcher
    {
        @Override
        public MockResponse dispatch(RecordedRequest request) throws InterruptedException
        {
            final String delegationToken = request.getHeaders().get(PRESTO_DELEGATION_TOKEN);
            if (!VALID_DELEGATION_TOKEN.equals(delegationToken)) {
                return new MockResponse().setStatus("HTTP/1.1 401 Not Authorized");
            }

            return super.dispatch(request);
        }
    }

    private List<String> createResults()
    {
        List<Column> columns = ImmutableList.of(new Column("_col0", "bigint", new ClientTypeSignature("bigint", ImmutableList.of())));
        return ImmutableList.<String>builder()
                .add(newQueryResults(1, null, null, "QUEUED"))
                .add(newQueryResults(2, columns, ImmutableList.of(ImmutableList.of(100)), "RUNNING"))
                .add(newQueryResults(null, columns, null, "FINISHED"))
                .build();
    }

    private String newQueryResults(Integer nextUriId, List<Column> responseColumns, List<List<Object>> data, String state)
    {
        String queryId = "20220211_214710_00012_rk69b";

        QueryResults queryResults = new QueryResults(
                queryId,
                serverInfo.server.url("/query.html?" + queryId).uri(),
                null,
                nextUriId == null ? null : serverInfo.server.url(format("/v1/statement/%s/%s", queryId, nextUriId)).uri(),
                responseColumns,
                data,
                new StatementStats(state, false, state.equals("QUEUED"), true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null),
                null,
                null,
                null,
                null);

        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }
}
