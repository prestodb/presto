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
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.spi.function.SqlFunction;
import okhttp3.OkHttpClient;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end smoke test that boots a real {@code presto_server} or {@code sapphire_cpp}
 * binary in metadata-sidecar mode, fetches {@code /v1/functions}, and asserts the registry
 * tool returns a substantial set of {@code SqlInvokedFunction} instances.
 *
 * <p>Skipped unless the {@code SAPPHIRE_CPP_BINARY} environment variable points to an
 * executable native worker binary. To run locally:
 *
 * <pre>{@code
 *   SAPPHIRE_CPP_BINARY=/home/<user>/cppbuild<date>/main \
 *     mvn -f .../presto-spark-base test -Dtest=TestMetadataSidecarProcessSmoke ...
 * }</pre>
 */
@Test(singleThreaded = true)
public class TestMetadataSidecarProcessSmoke
{
    private static final String BINARY_ENV_VAR = "SAPPHIRE_CPP_BINARY";

    private MetadataSidecarProcessFactory factory;
    private OkHttpClient httpClient;
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        String binary = System.getenv(BINARY_ENV_VAR);
        if (binary == null || !new File(binary).canExecute()) {
            throw new SkipException(BINARY_ENV_VAR + " env var must point to an executable sapphire_cpp / presto_server binary");
        }
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(60, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .build();
        executor = Executors.newSingleThreadExecutor();
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

        MetadataSidecarConfig config = new MetadataSidecarConfig()
                .setEnabled(true)
                .setExecutablePath(binary);
        factory = new MetadataSidecarProcessFactory(
                httpClient,
                executor,
                scheduledExecutor,
                jsonCodec(ServerInfo.class),
                config,
                Optional::empty);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (factory != null) {
            factory.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }

    @Test
    public void testEndToEndFunctionFetch()
    {
        JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> codec =
                mapJsonCodec(String.class, listJsonCodec(JsonBasedUdfFunctionMetadata.class));

        DriverSidecarFunctionRegistryTool tool =
                new DriverSidecarFunctionRegistryTool(httpClient, codec, factory);

        List<? extends SqlFunction> functions = tool.getWorkerFunctions();

        // We expect at least a few hundred — the actual fb_presto_cpp:main payload has ~2873
        // signatures across ~813 unique names, but be lenient here so the assertion still
        // holds for thinner OSS presto_server builds.
        assertThat(functions.size()).isGreaterThan(100);
        boolean hasPrestoDefault = functions.stream()
                .map(f -> f.getSignature().getName().toString())
                .anyMatch(name -> name.startsWith("presto.default."));
        assertThat(hasPrestoDefault).isTrue();
    }
}
