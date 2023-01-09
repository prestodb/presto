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
package com.facebook.presto.router;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.router.predictor.CpuInfo;
import com.facebook.presto.router.predictor.MemoryInfo;
import com.facebook.presto.router.predictor.PredictorManager;
import com.facebook.presto.router.predictor.ResourceGroup;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPredictorManager
{
    private static final int NUM_CLUSTERS = 2;

    private List<TestingPrestoServer> prestoServers;
    private LifeCycleManager lifeCycleManager;
    private PredictorManager predictorManager;
    private File configFile;
    private MockWebServer predictorServer;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        // set up server
        ImmutableList.Builder builder = ImmutableList.builder();
        for (int i = 0; i < NUM_CLUSTERS; ++i) {
            builder.add(createPrestoServer());
        }
        prestoServers = builder.build();
        configFile = getConfigFile(prestoServers);

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new RouterModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath())
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        predictorManager = injector.getInstance(PredictorManager.class);
        initializePredictorServer();
    }

    @Test(enabled = false)
    public void testPredictor()
    {
        String sql = "select * from presto.logs";

        ResourceGroup resourceGroup = predictorManager.fetchPrediction(sql).orElse(null);
        assertNotNull(resourceGroup, "The resource group should not be null");
        assertNotNull(resourceGroup.getCpuInfo());
        assertNotNull(resourceGroup.getMemoryInfo());

        resourceGroup = predictorManager.fetchPredictionParallel(sql).orElse(null);
        assertNotNull(resourceGroup, "The resource group should not be null");
        assertNotNull(resourceGroup.getCpuInfo());
        assertNotNull(resourceGroup.getMemoryInfo());

        CpuInfo cpuInfo = predictorManager.fetchCpuPrediction(sql).orElse(null);
        MemoryInfo memoryInfo = predictorManager.fetchMemoryPrediction(sql).orElse(null);
        assertNotNull(cpuInfo);
        assertNotNull(memoryInfo);

        int low = 0;
        int high = 3;
        assertTrue(low <= cpuInfo.getCpuTimeLabel(), "CPU time label should be larger or equal to " + low);
        assertTrue(cpuInfo.getCpuTimeLabel() <= high, "CPU time label should be smaller or equal to " + high);
        assertTrue(low <= memoryInfo.getMemoryBytesLabel(), "Memory bytes label should be larger or equal to " + low);
        assertTrue(memoryInfo.getMemoryBytesLabel() <= high, "Memory bytes label should be smaller or equal to " + high);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        for (TestingPrestoServer prestoServer : prestoServers) {
            prestoServer.close();
        }
        lifeCycleManager.stop();
        predictorServer.close();
    }

    private static TestingPrestoServer createPrestoServer()
            throws Exception
    {
        TestingPrestoServer server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.refreshNodes();

        return server;
    }

    private void initializePredictorServer()
            throws IOException
    {
        Dispatcher dispatcher = new Dispatcher()
        {
            @Override
            public MockResponse dispatch(RecordedRequest request)
            {
                switch (request.getPath()) {
                    case "/v1/cpu":
                        return new MockResponse()
                                .addHeader(CONTENT_TYPE, "application/json")
                                .setBody("{\"cpu_pred_label\": 2, \"cpu_pred_str\": \"1h - 5h\"}");
                    case "/v1/memory":
                        return new MockResponse()
                                .addHeader(CONTENT_TYPE, "application/json")
                                .setBody("{\"memory_pred_label\": 2, \"memory_pred_str\": \"> 1TB\"}");
                }
                return new MockResponse().setResponseCode(404);
            }
        };

        predictorServer = new MockWebServer();
        predictorServer.setDispatcher(dispatcher);
        predictorServer.start(8000);
    }

    private File getConfigFile(List<TestingPrestoServer> servers)
            throws IOException
    {
        // setup router config file
        File tempFile = File.createTempFile("router", "json");
        FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
        String configTemplate = new String(Files.readAllBytes(Paths.get(getResourceFilePath("simple-router-template.json"))));
        fileOutputStream.write(configTemplate.replaceAll("\\$\\{SERVERS}", getClusterList(servers)).getBytes(UTF_8));
        fileOutputStream.close();
        return tempFile;
    }

    private static String getClusterList(List<TestingPrestoServer> servers)
    {
        JsonCodec<List<URI>> codec = JsonCodec.listJsonCodec(URI.class);
        return codec.toJson(servers.stream().map(TestingPrestoServer::getBaseUrl).collect(toImmutableList()));
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
