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
package com.facebook.presto.server;

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.testing.Closeables;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_JOIN;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.spi.StandardErrorCode.INCOMPATIBLE_CLIENT;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.core.Response.Status.OK;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestServer
{
    private TestingPrestoServer server;
    private HttpClient client;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        client = new JettyHttpClient();
    }

    @SuppressWarnings("deprecation")
    @AfterMethod
    public void teardown()
    {
        Closeables.closeQuietly(server);
        Closeables.closeQuietly(client);
    }

    @Test
    public void testServerStarts()
            throws Exception
    {
        StatusResponseHandler.StatusResponse response = client.execute(
                prepareGet().setUri(server.resolve("/v1/query")).build(),
                createStatusResponseHandler());

        assertEquals(response.getStatusCode(), OK.getStatusCode());
    }

    @Test
    public void testQuery()
            throws Exception
    {
        // start query
        Request request = preparePost()
                .setUri(uriFor("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_CLIENT_INFO, "{\"clientVersion\":\"testVersion\"}")
                .addHeader(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                .addHeader(PRESTO_SESSION, DISTRIBUTED_JOIN + "=true," + HASH_PARTITION_COUNT + " = 43")
                .addHeader(PRESTO_PREPARED_STATEMENT, "foo=select * from bar")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));

        // get the query info
        QueryInfo queryInfo = server.getQueryManager().getQueryInfo(new QueryId(queryResults.getId()));

        // verify session properties
        assertEquals(queryInfo.getSession().getSystemProperties(), ImmutableMap.builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(DISTRIBUTED_JOIN, "true")
                .put(HASH_PARTITION_COUNT, "43")
                .build());

        // verify client info in session
        assertEquals(queryInfo.getSession().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");

        // verify prepared statements
        assertEquals(queryInfo.getSession().getPreparedStatements(), ImmutableMap.builder()
                .put("foo", "select * from bar")
                .build());

        ImmutableList.Builder<List<Object>> data = ImmutableList.builder();
        if (queryResults.getData() != null) {
            data.addAll(queryResults.getData());
        }

        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(jsonCodec(QueryResults.class)));

            if (queryResults.getData() != null) {
                data.addAll(queryResults.getData());
            }
        }
        assertNull(queryResults.getError());

        // only the system catalog exists by default
        List<List<Object>> rows = data.build();
        assertEquals(rows, ImmutableList.of(ImmutableList.of("system")));
    }

    @Test
    public void testTransactionSupport()
            throws Exception
    {
        Request request = preparePost()
                .setUri(uriFor("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_TRANSACTION_ID, "none")
                .build();

        JsonResponse<QueryResults> queryResults = client.execute(request, createFullJsonResponseHandler(jsonCodec(QueryResults.class)));
        ImmutableList.Builder<List<Object>> data = ImmutableList.builder();
        while (true) {
            if (queryResults.getValue().getData() != null) {
                data.addAll(queryResults.getValue().getData());
            }

            if (queryResults.getValue().getNextUri() == null) {
                break;
            }
            queryResults = client.execute(prepareGet().setUri(queryResults.getValue().getNextUri()).build(), createFullJsonResponseHandler(jsonCodec(QueryResults.class)));
        }
        assertNull(queryResults.getValue().getError());
        assertNotNull(queryResults.getHeader(PRESTO_STARTED_TRANSACTION_ID));
    }

    @Test
    public void testNoTransactionSupport()
            throws Exception
    {
        Request request = preparePost()
                .setUri(uriFor("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(jsonCodec(QueryResults.class)));
        }

        assertNotNull(queryResults.getError());
        assertEquals(queryResults.getError().getErrorCode(), INCOMPATIBLE_CLIENT.toErrorCode().getCode());
    }

    public URI uriFor(String path)
    {
        return HttpUriBuilder.uriBuilderFrom(server.getBaseUrl()).replacePath(path).build();
    }
}
