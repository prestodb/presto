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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.Request.Builder.preparePut;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREFIX_URL;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCatalogResource
{
    private static final String CATALOG_RESOURCE_URI = "/v1/catalog/%s";
    private static final JsonCodec<List<String>> LIST_CODEC = listJsonCodec(String.class);
    private HttpClient client;
    private DistributedQueryRunner runner;
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        runner = createQueryRunner();
        server = runner.getCoordinator();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
        server = null;
        client = null;
    }

    @Test
    public void testCatalogResource()
            throws Exception
    {
        final Map<String, String> catalogProperties = getCatalogPropertiesMap();
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve(format(CATALOG_RESOURCE_URI, "tpch_1"))).build();

        // CREATE FLOW
        Request.Builder requestBuiler = setContentTypeHeaders(false, preparePost());
        Request request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 201);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 409);
        validateCatalogDetails(ImmutableSet.of("system", "testing_catalog", "tpch", "tpch_1"));

        // DELETE FLOW
        requestBuiler = setContentTypeHeaders(false, prepareDelete());
        request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 204);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 409);
        validateCatalogDetails(ImmutableSet.of("system", "testing_catalog", "tpch"));

        // UPDATE FLOW
        requestBuiler = setContentTypeHeaders(false, preparePut());
        request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 201);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 204);
        validateCatalogDetails(ImmutableSet.of("system", "testing_catalog", "tpch", "tpch_1"));

        request = prepareGet().setUri(server.getBaseUrl().resolve("/v1/catalog")).build();
        List<String> catalogList = client.execute(request, createJsonResponseHandler(LIST_CODEC));
        assertTrue(!catalogList.isEmpty());
        assertTrue(catalogList.contains("tpch_1"));
    }

    @Test
    public void testInvalidCatalogName()
    {
        final Map<String, String> catalogProperties = getCatalogPropertiesMap();
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve(format(CATALOG_RESOURCE_URI, "UNKNOWN88"))).build();
        Request.Builder requestBuiler = setContentTypeHeaders(false, preparePost());
        Request request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 404);
    }

    @Test(enabled = false)
    public void testConcurrentQueryRemoveCatalog() throws InterruptedException
    {
        final Map<String, String> catalogProperties = getCatalogPropertiesMap();
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve(format(CATALOG_RESOURCE_URI, "tpch_1"))).build();
        Request.Builder requestBuiler = setContentTypeHeaders(false, preparePost());
        Request request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 201);

        Function<String, Exception> catalogQueryFunction = (catalog) -> {
            try {
                runner.execute("SELECT * FROM " + catalog + ".tiny.orders");
            }
            catch (Exception e) {
                return e;
            }
            return null;
        };
        ConcurrentQueryRunner<String, Exception> runner = new ConcurrentQueryRunner<>(4, catalogQueryFunction, "tpch_1");

        runner.start();
        Thread.sleep(1000);
        requestBuiler = setContentTypeHeaders(false, prepareDelete());
        request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 409);
        runner.stop();
    }

    @Test
    public void testUnrelatedCatalogRemoval() throws InterruptedException
    {
        final Map<String, String> catalogProperties = getCatalogPropertiesMap();
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve(format(CATALOG_RESOURCE_URI, "tpch_3"))).build();
        Request.Builder requestBuiler = setContentTypeHeaders(false, preparePost());
        Request request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 201);

        uri = uriBuilderFrom(server.getBaseUrl().resolve(format(CATALOG_RESOURCE_URI, "tpch_4"))).build();
        requestBuiler = setContentTypeHeaders(false, preparePost());
        request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 201);

        Function<String, Exception> catalogQueryFunction = (catalog) -> {
            try {
                runner.execute("SELECT * FROM " + catalog + ".tiny.orders");
            }
            catch (Exception e) {
                return e;
            }
            return null;
        };
        ConcurrentQueryRunner<String, Exception> runner = new ConcurrentQueryRunner<>(4, catalogQueryFunction, "tpch_3");

        runner.start();
        Thread.sleep(1000);
        requestBuiler = setContentTypeHeaders(false, prepareDelete());
        request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 204);
        runner.stop();
    }

    @Test(enabled = false)
    public void testConcurrentCatalogManipulation() throws InterruptedException
    {
        final Map<String, String> catalogProperties = getCatalogPropertiesMap();
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve(format(CATALOG_RESOURCE_URI, "tpch_1"))).build();
        Request.Builder requestBuiler = setContentTypeHeaders(false, preparePost());
        Request request = createRequest(requestBuiler, catalogProperties, uri);
        assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 201);
        Thread appendThread = new Thread() {
            public void run()
            {
                URI uri = uriBuilderFrom(server.getBaseUrl().resolve(format(CATALOG_RESOURCE_URI, "tpch_2"))).build();
                Request.Builder requestBuiler = setContentTypeHeaders(false, preparePost());
                Request request = createRequest(requestBuiler, catalogProperties, uri);
                assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 201);
            }
        };

        Thread deleteThread = new Thread() {
            public void run()
            {
                Request.Builder requestBuiler = setContentTypeHeaders(false, prepareDelete());
                Request request = createRequest(requestBuiler, catalogProperties, uri);
                assertEquals(client.execute(request, createStringResponseHandler()).getStatusCode(), 204);
            }
        };
        appendThread.start();
        deleteThread.start();
        appendThread.join();
        deleteThread.join();
        validateCatalogDetails(ImmutableSet.of("system", "testing_catalog", "tpch", "tpch_2"));
    }

    private void validateCatalogDetails(Set<String> expectedCatalogs)
    {
        MaterializedResult result = runner.execute(runner.getDefaultSession(), "SHOW CATALOGS").toTestTypes();
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedCatalogs));
    }

    private Request createRequest(Request.Builder requestBuiler, Map<String, String> catalogProperties, URI uri)
    {
        return requestBuiler.setHeader(PRESTO_USER, "PRESTO_USER")
                .setHeader(PRESTO_PREFIX_URL, "PREFIX")
                .setUri(uri)
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(Map.class), catalogProperties))
                .build();
    }

    private static Map<String, String> getCatalogPropertiesMap()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.name", "tpch");
        properties.put("tpch.splits-per-node", "4");
        return properties;
    }
    private class ConcurrentQueryRunner<T, R>
    {
        private final ExecutorService executorService;
        private final Function<T, R> function;
        private final T input;
        private final int concurrencyLevel;

        public ConcurrentQueryRunner(int concurrencyLevel, Function<T, R> function, T input)
        {
            this.executorService = Executors.newFixedThreadPool(concurrencyLevel);
            this.function = function;
            this.input = input;
            this.concurrencyLevel = concurrencyLevel;
        }

        public void start()
        {
            for (int i = 0; i < concurrencyLevel; i++) {
                submitTask();
            }
        }

        private void submitTask()
        {
            CompletableFuture.supplyAsync(() -> function.apply(input), executorService).whenComplete((x, y) -> submitTask());
        }

        public void stop()
        {
            executorService.shutdownNow();
        }
    }
}
