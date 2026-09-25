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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunction;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestDriverSidecarFunctionRegistryTool
{
    private static final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> CODEC =
            mapJsonCodec(String.class, listJsonCodec(JsonBasedUdfFunctionMetadata.class));

    private HttpServer server;
    private URI baseUri;
    private AtomicInteger fetchCount;

    @BeforeMethod
    public void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        baseUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        fetchCount = new AtomicInteger();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testFetchAndConvert()
    {
        String body = "{" +
                "\"my_scalar\": [{" +
                "  \"docString\":\"presto.default.my_scalar\"," +
                "  \"functionKind\":\"SCALAR\"," +
                "  \"outputType\":\"bigint\"," +
                "  \"paramTypes\":[\"bigint\"]," +
                "  \"schema\":\"default\"," +
                "  \"variableArity\":false," +
                "  \"longVariableConstraints\":[]," +
                "  \"typeVariableConstraints\":[]," +
                "  \"routineCharacteristics\":{" +
                "    \"determinism\":\"DETERMINISTIC\"," +
                "    \"language\":\"CPP\"," +
                "    \"nullCallClause\":\"RETURNS_NULL_ON_NULL_INPUT\"" +
                "  }" +
                "}]" +
                "}";
        bindEndpoint("/v1/functions", body);

        DriverSidecarFunctionRegistryTool tool = newTool();

        List<? extends SqlFunction> functions = tool.getWorkerFunctions();
        assertThat(functions).hasSize(1);
        assertThat(functions.get(0).getSignature().getName().toString())
                .isEqualTo("presto.default.my_scalar");
    }

    @Test
    public void testCachedAcrossCalls()
    {
        bindEndpoint("/v1/functions", "{}");
        DriverSidecarFunctionRegistryTool tool = newTool();

        tool.getWorkerFunctions();
        tool.getWorkerFunctions();
        tool.getRpcFunctionNames();

        assertThat(fetchCount.get()).isEqualTo(1);
    }

    @Test
    public void testHttpErrorThrowsPrestoException()
    {
        server.createContext("/v1/functions", exchange -> {
            fetchCount.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();

        DriverSidecarFunctionRegistryTool tool = newTool();

        assertThatThrownBy(tool::getWorkerFunctions)
                .isInstanceOf(PrestoException.class)
                .hasMessageContaining("HTTP 500");
    }

    @Test
    public void testRpcFunctionNamesPicksOnlyRpcEntries()
    {
        String body = "{" +
                "\"plain_fn\": [" + scalarMetaFragment(false) + "]," +
                "\"RPC_FN\": [" + scalarMetaFragment(true) + "]" +
                "}";
        bindEndpoint("/v1/functions", body);

        DriverSidecarFunctionRegistryTool tool = newTool();

        assertThat(tool.getRpcFunctionNames()).containsExactly("rpc_fn");
    }

    private void bindEndpoint(String path, String body)
    {
        HttpHandler handler = new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange)
                    throws IOException
            {
                fetchCount.incrementAndGet();
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
        };
        server.createContext(path, handler);
        server.start();
    }

    private DriverSidecarFunctionRegistryTool newTool()
    {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build();
        FixedUriSidecarProcessFactory factory = new FixedUriSidecarProcessFactory(baseUri);
        return new DriverSidecarFunctionRegistryTool(client, CODEC, factory);
    }

    private static String scalarMetaFragment(boolean rpc)
    {
        return "{" +
                "  \"docString\":\"\"," +
                "  \"functionKind\":\"SCALAR\"," +
                "  \"outputType\":\"bigint\"," +
                "  \"paramTypes\":[]," +
                "  \"schema\":\"default\"," +
                "  \"variableArity\":false," +
                "  \"longVariableConstraints\":[]," +
                "  \"typeVariableConstraints\":[]," +
                "  \"isRpcFunction\":" + rpc + "," +
                "  \"routineCharacteristics\":{" +
                "    \"determinism\":\"DETERMINISTIC\"," +
                "    \"language\":\"CPP\"," +
                "    \"nullCallClause\":\"RETURNS_NULL_ON_NULL_INPUT\"" +
                "  }" +
                "}";
    }

    /**
     * Test stand-in for {@link MetadataSidecarProcessFactory} that returns a fixed URI rather
     * than spawning a process. We cannot easily mock the real factory here because it has a
     * non-trivial constructor wiring; this avoids that complexity.
     */
    private static class FixedUriSidecarProcessFactory
            extends MetadataSidecarProcessFactory
    {
        private final URI uri;

        FixedUriSidecarProcessFactory(URI uri)
        {
            super(
                    new OkHttpClient(),
                    java.util.concurrent.Executors.newSingleThreadExecutor(),
                    java.util.concurrent.Executors.newSingleThreadScheduledExecutor(),
                    JsonCodec.jsonCodec(com.facebook.presto.client.ServerInfo.class),
                    new MetadataSidecarConfig().setExecutablePath("/bin/echo"),
                    java.util.Optional::empty);
            this.uri = uri;
        }

        @Override
        public URI getOrStart()
        {
            return uri;
        }
    }
}
