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

import com.facebook.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.StatusResponseHandler;
import com.facebook.airlift.http.client.UnexpectedResponseException;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.testing.Closeables;
import com.facebook.presto.CompressionCodec;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.TimeZoneNotSupportedException;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Base64;
import java.util.List;

import static com.facebook.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.fromRequest;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.prepareHead;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.Request.Builder.preparePut;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_RETRY_QUERY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.server.TestHttpRequestSessionContext.createFunctionAdd;
import static com.facebook.presto.server.TestHttpRequestSessionContext.createSqlFunctionIdAdd;
import static com.facebook.presto.server.TestHttpRequestSessionContext.urlEncode;
import static com.facebook.presto.spi.StandardErrorCode.INCOMPATIBLE_CLIENT;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPage;
import static jakarta.ws.rs.core.HttpHeaders.CACHE_CONTROL;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.OK;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestServer
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private static final SqlFunctionId SQL_FUNCTION_ID_ADD = createSqlFunctionIdAdd();
    private static final SqlInvokedFunction SQL_FUNCTION_ADD = createFunctionAdd();
    private static final String SERIALIZED_SQL_FUNCTION_ID_ADD = jsonCodec(SqlFunctionId.class).toJson(SQL_FUNCTION_ID_ADD);
    private static final String SERIALIZED_SQL_FUNCTION_ADD = jsonCodec(SqlInvokedFunction.class).toJson(SQL_FUNCTION_ADD);

    private TestingPrestoServer server;
    private HttpClient client;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        client = new JettyHttpClient();
    }

    @AfterMethod
    public void teardown()
    {
        Closeables.closeQuietly(server);
        Closeables.closeQuietly(client);
    }

    @Test
    public void testInvalidSessionError()
    {
        String invalidTimeZone = "this_is_an_invalid_time_zone";
        Request request = preparePost().setHeader(PRESTO_USER, "user")
                .setUri(uriFor("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_TIME_ZONE, invalidTimeZone)
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        }
        QueryError queryError = queryResults.getError();
        assertNotNull(queryError);

        TimeZoneNotSupportedException expected = new TimeZoneNotSupportedException(invalidTimeZone);
        assertEquals(queryError.getMessage(), expected.getMessage());
    }

    @Test
    public void testServerStarts()
    {
        StatusResponseHandler.StatusResponse response = client.execute(
                prepareGet().setUri(server.resolve("/v1/query")).build(),
                createStatusResponseHandler());

        assertEquals(response.getStatusCode(), OK.getStatusCode());
    }

    @Test
    public void testBinaryResults()
    {
        // start query
        URI uri = buildStatementUri(true);
        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_CLIENT_INFO, "{\"clientVersion\":\"testVersion\"}")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));

        ImmutableList.Builder<Object> data = ImmutableList.builder();
        while (queryResults.getNextUri() != null) {
            Request nextRequest = prepareGet()
                    .setUri(queryResults.getNextUri())
                    .build();
            queryResults = client.execute(nextRequest, createJsonResponseHandler(QUERY_RESULTS_CODEC));

            assertNull(queryResults.getData());
            if (queryResults.getBinaryData() != null) {
                data.addAll(queryResults.getBinaryData());
            }
        }

        if (queryResults.getError() != null) {
            fail(queryResults.getError().toString());
        }

        List<Object> encodedPages = data.build();

        assertEquals(1, encodedPages.size());
        byte[] decodedPage = Base64.getDecoder().decode((String) encodedPages.get(0));

        BlockEncodingManager blockEncodingSerde = new BlockEncodingManager();
        PagesSerde pagesSerde = new PagesSerdeFactory(blockEncodingSerde, CompressionCodec.NONE, false).createPagesSerde();
        BasicSliceInput pageInput = new BasicSliceInput(Slices.wrappedBuffer(decodedPage, 0, decodedPage.length));
        SerializedPage serializedPage = readSerializedPage(pageInput);

        Page page = pagesSerde.deserialize(serializedPage);

        assertEquals(1, page.getChannelCount());
        assertEquals(1, page.getPositionCount());

        // only the system catalog exists by default
        Slice slice = VARCHAR.getSlice(page.getBlock(0), 0);
        assertEquals(slice.toStringUtf8(), "system");
    }

    @Test
    public void testQuery()
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
                .addHeader(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
                .addHeader(PRESTO_PREPARED_STATEMENT, "foo=select * from bar")
                .addHeader(PRESTO_SESSION_FUNCTION, format("%s=%s", urlEncode(SERIALIZED_SQL_FUNCTION_ID_ADD), urlEncode(SERIALIZED_SQL_FUNCTION_ADD)))
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));

        ImmutableList.Builder<List<Object>> data = ImmutableList.builder();
        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));

            if (queryResults.getData() != null) {
                data.addAll(queryResults.getData());
            }
        }

        if (queryResults.getError() != null) {
            fail(queryResults.getError().toString());
        }

        // get the query info
        BasicQueryInfo queryInfo = server.getQueryManager().getQueryInfo(new QueryId(queryResults.getId()));

        // verify session properties
        assertEquals(queryInfo.getSession().getSystemProperties(), ImmutableMap.builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(HASH_PARTITION_COUNT, "43")
                .build());

        // verify client info in session
        assertEquals(queryInfo.getSession().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");

        // verify prepared statements
        assertEquals(queryInfo.getSession().getPreparedStatements(), ImmutableMap.builder()
                .put("foo", "select * from bar")
                .build());

        // verify session functions
        assertEquals(queryInfo.getSession().getSessionFunctions(), ImmutableMap.of(SQL_FUNCTION_ID_ADD, SQL_FUNCTION_ADD));

        // only the system catalog exists by default
        List<List<Object>> rows = data.build();
        assertEquals(rows, ImmutableList.of(ImmutableList.of("system")));
    }

    @Test
    public void testQueryWithPreMintedQueryIdAndSlug()
    {
        QueryId queryId = new QueryIdGenerator().createNextQueryId();
        String slug = "xxx";
        Request request = preparePut()
                .setUri(uriFor("/v1/statement/", queryId, slug))
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));

        // verify slug in nextUri is same as requested
        assertEquals(queryResults.getNextUri().getQuery(), "slug=xxx");

        // verify nextUri points to requested query id
        assertEquals(queryResults.getNextUri().getPath(), format("/v1/statement/queued/%s/1", queryId));

        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        }

        if (queryResults.getError() != null) {
            fail(queryResults.getError().toString());
        }

        // verify query id was passed down properly
        assertEquals(server.getDispatchManager().getQueryInfo(queryId).getQueryId(), queryId);
    }

    @Test
    public void testPutStatementIdempotency()
    {
        QueryId queryId = new QueryIdGenerator().createNextQueryId();
        Request request = preparePut()
                .setUri(uriFor("/v1/statement/", queryId, "slug"))
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .build();

        client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
        // Execute PUT request again should succeed
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));

        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        }
        if (queryResults.getError() != null) {
            fail(queryResults.getError().toString());
        }
    }

    @Test(expectedExceptions = UnexpectedResponseException.class, expectedExceptionsMessageRegExp = "Expected response code to be \\[.*\\], but was 409")
    public void testPutStatementWithDifferentSlugFails()
    {
        QueryId queryId = new QueryIdGenerator().createNextQueryId();
        Request request = preparePut()
                .setUri(uriFor("/v1/statement/", queryId, "slug"))
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .build();
        client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));

        Request badRequest = fromRequest(request)
                .setUri(uriFor("/v1/statement/", queryId, "different_slug"))
                .build();
        client.execute(badRequest, createJsonResponseHandler(QUERY_RESULTS_CODEC));
    }

    @Test(expectedExceptions = UnexpectedResponseException.class, expectedExceptionsMessageRegExp = "Expected response code to be \\[.*\\], but was 409")
    public void testPutStatementAfterGetFails()
    {
        QueryId queryId = new QueryIdGenerator().createNextQueryId();
        Request request = preparePut()
                .setUri(uriFor("/v1/statement/", queryId, "slug"))
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
        client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
    }

    @Test
    public void testTransactionSupport()
    {
        Request request = preparePost()
                .setUri(uriFor("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_TRANSACTION_ID, "none")
                .build();

        JsonResponse<QueryResults> queryResults = client.execute(request, createFullJsonResponseHandler(QUERY_RESULTS_CODEC));
        ImmutableList.Builder<List<Object>> data = ImmutableList.builder();
        while (true) {
            if (queryResults.getValue().getData() != null) {
                data.addAll(queryResults.getValue().getData());
            }

            if (queryResults.getValue().getNextUri() == null) {
                break;
            }
            queryResults = client.execute(prepareGet().setUri(queryResults.getValue().getNextUri()).build(), createFullJsonResponseHandler(QUERY_RESULTS_CODEC));
        }

        if (queryResults.getValue().getError() != null) {
            fail(queryResults.getValue().getError().toString());
        }
        assertNotNull(queryResults.getHeader(PRESTO_STARTED_TRANSACTION_ID));
    }

    @Test
    public void testNoTransactionSupport()
    {
        Request request = preparePost()
                .setUri(uriFor("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        }

        assertNotNull(queryResults.getError());
        assertEquals(queryResults.getError().getErrorCode(), INCOMPATIBLE_CLIENT.toErrorCode().getCode());
    }

    @Test
    public void testStatusPing()
    {
        Request request = prepareHead()
                .setUri(uriFor("/v1/status"))
                .setFollowRedirects(false)
                .build();
        StatusResponseHandler.StatusResponse response = client.execute(request, createStatusResponseHandler());
        assertEquals(response.getStatusCode(), OK.getStatusCode(), "Status code");
        assertEquals(response.getHeader(CONTENT_TYPE), APPLICATION_JSON, "Content Type");
    }

    @Test
    public void testCacheControlHeaderExists()
    {
        Request request = preparePost()
                .setUri(uriFor("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .build();

        JsonResponse<QueryResults> initResponse = client.execute(request, createFullJsonResponseHandler(QUERY_RESULTS_CODEC));

        String initHeader = initResponse.getHeader(CACHE_CONTROL);
        assertNotNull(initHeader);
        assertTrue(initHeader.contains("max-age"));

        int initAge = parseInt(initHeader.substring(initHeader.indexOf("=") + 1));
        assertTrue(initAge >= 0);

        JsonResponse<QueryResults> queryResults = initResponse;
        while (queryResults.getValue().getNextUri() != null) {
            URI nextUri = queryResults.getValue().getNextUri();
            queryResults = client.execute(prepareGet().setUri(nextUri).build(), createFullJsonResponseHandler(QUERY_RESULTS_CODEC));

            String header = queryResults.getHeader(CACHE_CONTROL);
            assertNotNull(header);
            assertTrue(header.contains("max-age"));

            int maxAge = parseInt(header.substring(header.indexOf("=") + 1));
            assertTrue(maxAge >= 0);
        }
    }

    @Test
    public void testQueryWithRetryUrl()
    {
        String retryUrl = format("%s/v1/statement/queued/retry/abc123", server.getBaseUrl());
        int expirationSeconds = 3600;

        // start query with retry URL
        URI uri = buildStatementUri(retryUrl, expirationSeconds, false);

        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator("SELECT 123", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
        assertNotNull(queryResults);
        assertNotNull(queryResults.getId());

        // Verify query executes normally with retry parameters
        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        }
        assertNull(queryResults.getError());
    }

    @Test
    public void testQueryWithRetryFlag()
    {
        // submit a retry query (marked with retry flag)
        URI uri = buildStatementUri();

        String retryQuery = "-- retry query 20240115_120000_00000_xxxxx; attempt: 1\nSELECT 456";
        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(retryQuery, UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_RETRY_QUERY, "true")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
        assertNotNull(queryResults);
        assertNotNull(queryResults.getId());

        // Verify the usual endpoint fails
        try {
            client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        }
        catch (UnexpectedResponseException e) {
            // Expected failure, retry query should not be fetched from the usual endpoint
            assertEquals(e.getStatusCode(), 409, "Expected 409 Conflict for retry query");
        }

        request = prepareGet()
                .setUri(HttpUriBuilder.uriBuilderFrom(server.getBaseUrl())
                        .replacePath(format("/v1/statement/queued/retry/%s", queryResults.getId()))
                        .build())
                .build();
        // Fetch the retry query results from the special retry endpoint
        queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));

        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        }

        assertNull(queryResults.getError());
    }

    @Test
    public void testQueryWithInvalidRetryParameters()
    {
        // Test with missing expiration when retry URL is provided
        String retryUrl = format("%s/v1/statement/queued/retry", server.getBaseUrl());
        URI uri = buildStatementUri(retryUrl, null, false);

        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator("SELECT 1", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .build();

        JsonResponse<QueryResults> response = client.execute(request, createFullJsonResponseHandler(QUERY_RESULTS_CODEC));
        assertEquals(response.getStatusCode(), 400);
    }

    @Test
    public void testQueryWithInvalidRetryUrl()
    {
        // Test with invalid remote retry URL
        String invalidRetryUrl = "http://insecure-cluster.example.com/v1/statement";
        URI uri = buildStatementUri(invalidRetryUrl, 3600, false);

        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator("SELECT 1", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .build();

        JsonResponse<QueryResults> response = client.execute(request, createFullJsonResponseHandler(QUERY_RESULTS_CODEC));
        assertEquals(response.getStatusCode(), 400);
    }

    @Test
    public void testQueryWithExpiredRetryTime()
    {
        String retryUrl = "https://backup-cluster.example.com/v1/statement";
        // Use 0 seconds expiration (immediately expired)
        URI uri = buildStatementUri(retryUrl, 0, false);

        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator("SELECT 1", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .build();

        JsonResponse<QueryResults> response = client.execute(request, createFullJsonResponseHandler(QUERY_RESULTS_CODEC));
        assertEquals(response.getStatusCode(), 400);
    }

    @Test
    public void testQueryWithRetryUrlAndSessionProperties()
    {
        String retryUrl = format("%s/v1/statement/queued/retry", server.getBaseUrl());
        URI uri = buildStatementUri(retryUrl, 3600, false);

        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator("SELECT 789", UTF_8))
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source")
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                .setHeader(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=BROADCAST")
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
        assertNotNull(queryResults);
        assertNotNull(queryResults.getId());

        // Verify query executes with both retry parameters and session properties
        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
        }
        assertNull(queryResults.getError());
    }

    @Test
    public void testCrossClusterRetryWithRetryChainPrevention()
            throws Exception
    {
        // Create two test servers to simulate two clusters
        TestingPrestoServer backupServer = null;
        HttpClient backupClient = null;
        try {
            backupServer = new TestingPrestoServer();
            backupClient = new JettyHttpClient();

            // Step 1: Simulate router POSTing query to backup server with retry header
            URI backupUri = HttpUriBuilder.uriBuilderFrom(backupServer.getBaseUrl())
                    .replacePath("/v1/statement")
                    .build();

            String failQuery = format("SELECT fail(%s, 'Simulated remote task error')", REMOTE_TASK_ERROR.toErrorCode().getCode());
            Request backupRequest = preparePost()
                    .setUri(backupUri)
                    .setBodyGenerator(createStaticBodyGenerator(failQuery, UTF_8))
                    .setHeader(PRESTO_USER, "user")
                    .setHeader(PRESTO_SOURCE, "source")
                    .setHeader(PRESTO_CATALOG, "catalog")
                    .setHeader(PRESTO_SCHEMA, "schema")
                    .setHeader(PRESTO_SESSION, "query_retry_limit=1")
                    .setHeader(PRESTO_RETRY_QUERY, "true")  // Router marks this as a retry query
                    .build();

            // Router POSTs to backup server and gets the query ID for the retry query
            QueryResults backupResults = backupClient.execute(backupRequest, createJsonResponseHandler(QUERY_RESULTS_CODEC));
            assertNotNull(backupResults);
            String retryQueryId = backupResults.getId();

            // Step 2: Configure retry URL to point to the backup cluster's retry endpoint
            String retryUrl = backupServer.getBaseUrl() + "/v1/statement/queued/retry/" + retryQueryId;

            // Step 3: Router redirects client to primary server with retry parameters
            URI uri = buildStatementUri(retryUrl, 3600, false);

            Request request = preparePost()
                    .setUri(uri)
                    .setBodyGenerator(createStaticBodyGenerator(failQuery, UTF_8))
                    .setHeader(PRESTO_USER, "user")
                    .setHeader(PRESTO_SOURCE, "source")
                    .setHeader(PRESTO_CATALOG, "catalog")
                    .setHeader(PRESTO_SCHEMA, "schema")
                    .setHeader(PRESTO_SESSION, "query_retry_limit=1")
                    .build();

            // Execute the query on the primary cluster
            QueryResults firstResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));
            assertNotNull(firstResults);

            // Poll for results until we get the error or nextUri changes to backup cluster
            int iterations = 0;
            while (firstResults.getNextUri() != null && iterations < 20) {
                // Check if nextUri has changed from primary to backup cluster
                URI nextUri = firstResults.getNextUri();
                URI primaryUri = URI.create(server.getBaseUrl().toString());
                URI backupClusterUri = URI.create(backupServer.getBaseUrl().toString());

                // Check both host and port to determine if we've switched clusters
                boolean isPrimaryCluster = nextUri.getHost().equals(primaryUri.getHost()) &&
                                         nextUri.getPort() == primaryUri.getPort();
                boolean isBackupCluster = nextUri.getHost().equals(backupClusterUri.getHost()) &&
                                        nextUri.getPort() == backupClusterUri.getPort();

                if (!isPrimaryCluster && isBackupCluster) {
                    // NextUri has changed to backup cluster - this is expected
                    break;
                }

                // If we have an error, check if it includes the retry URL
                if (firstResults.getError() != null) {
                    // Even with an error, there should be a nextUri pointing to backup cluster
                    assertNotNull(firstResults.getNextUri(), "Error response should include retry URL");
                    assertTrue(firstResults.getNextUri().toString().contains(backupServer.getBaseUrl().toString()),
                            "Error response should have nextUri pointing to backup cluster");
                    break;
                }

                firstResults = client.execute(prepareGet().setUri(firstResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
                iterations++;
            }

            // Verify we didn't timeout
            assertTrue(iterations < 20, "Query did not complete or redirect within 20 iterations");

            // Verify the query failure is not exposed yet
            assertNull(firstResults.getError());

            // The nextUri should now point to the backup cluster
            assertNotNull(firstResults.getNextUri());
            assertTrue(firstResults.getNextUri().toString().contains(backupServer.getBaseUrl().toString()));

            // Step 4: Client follows the redirect link from primary server to backup server
            // The retry endpoint will return the results of the retry query that was already created
            Request retryRequest = prepareGet()
                    .setUri(firstResults.getNextUri())
                    .build();

            QueryResults retryResults = backupClient.execute(retryRequest, createJsonResponseHandler(QUERY_RESULTS_CODEC));
            assertNotNull(retryResults);

            // Verify the retry query ID does not match what we created earlier (because the retry query is a placeholder,
            // and when it is retried it generates a new ID)
            assertNotEquals(retryResults.getId(), retryQueryId);

            // The retry query should also fail (since we're using the same fail() function)
            while (retryResults.getNextUri() != null) {
                retryResults = backupClient.execute(prepareGet().setUri(retryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_CODEC));
            }

            // Verify the retry query also failed
            assertNotNull(retryResults.getError());
            assertEquals(retryResults.getError().getErrorName(), "REMOTE_TASK_ERROR");

            // IMPORTANT: The retry query should NOT have a nextUri for another retry
            // This prevents retry chains (retry of a retry)
            assertNull(retryResults.getNextUri(), "Retry query should not have nextUri to prevent retry chains");
        }
        finally {
            Closeables.closeQuietly(backupClient);
            Closeables.closeQuietly(backupServer);
        }
    }

    public URI uriFor(String path)
    {
        return HttpUriBuilder.uriBuilderFrom(server.getBaseUrl()).replacePath(path).build();
    }

    public URI uriFor(String path, QueryId queryId, String slug)
    {
        return HttpUriBuilder.uriBuilderFrom(server.getBaseUrl())
                .replacePath(path)
                .appendPath(queryId.getId())
                .addParameter("slug", slug)
                .build();
    }

    private URI buildStatementUri()
    {
        return buildStatementUri(null, null, false);
    }

    private URI buildStatementUri(boolean binaryResults)
    {
        return buildStatementUri(null, null, binaryResults);
    }

    private URI buildStatementUri(String retryUrl, Integer retryExpirationInSeconds, boolean binaryResults)
    {
        HttpUriBuilder builder = HttpUriBuilder.uriBuilderFrom(server.getBaseUrl())
                .replacePath("/v1/statement");

        if (retryUrl != null) {
            builder.addParameter("retryUrl", urlEncode(retryUrl));
        }

        if (retryExpirationInSeconds != null) {
            builder.addParameter("retryExpirationInSeconds", String.valueOf(retryExpirationInSeconds));
        }

        if (binaryResults) {
            builder.replaceParameter("binaryResults", "true");
        }

        return builder.build();
    }
}
