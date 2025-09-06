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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFatalException;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcess;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcessFactory;
import com.facebook.presto.spark.execution.property.NativeExecutionCatalogProperties;
import com.facebook.presto.spark.execution.property.NativeExecutionNodeConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.PrestoSparkWorkerProperty;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableMap;
import okhttp3.OkHttpClient;
import org.apache.spark.SparkConf;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestNativeExecutionProcess
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_JSON_CODEC = JsonCodec.jsonCodec(ServerInfo.class);

    @Test
    public void testNativeProcessIsAlive()
    {
        Session session = testSessionBuilder().build();
        NativeExecutionProcessFactory factory = createNativeExecutionProcessFactory();
        NativeExecutionProcess process = factory.getNativeExecutionProcess(session);
        // Simulate the process is closed (crashed)
        process.close();
        assertFalse(process.isAlive());
    }

    @Test
    public void testNativeProcessRelaunch()
    {
        Session session = testSessionBuilder().build();
        NativeExecutionProcessFactory factory = createNativeExecutionProcessFactory();
        NativeExecutionProcess process = factory.getNativeExecutionProcess(session);
        // Simulate the process is closed (crashed)
        process.close();
        assertFalse(process.isAlive());
        NativeExecutionProcess process2 = factory.getNativeExecutionProcess(session);
        // Expecting the factory re-created a new process object so that the process and process2
        // should be two different objects
        assertNotSame(process2, process);
    }

    @Test
    public void testNativeProcessShutdown()
    {
        Session session = testSessionBuilder().build();
        NativeExecutionProcessFactory factory = createNativeExecutionProcessFactory();
        // Set the maxRetryDuration to 0 ms to allow the RequestErrorTracker failing immediately
        NativeExecutionProcess process = factory.createNativeExecutionProcess(session, new Duration(0, TimeUnit.MILLISECONDS));
        Throwable exception = expectThrows(PrestoSparkFatalException.class, process::start);
        assertTrue(exception.getMessage().contains("Native process launch failed with multiple retries"));
        assertFalse(process.isAlive());
    }

    @Test
    public void testUpdateWorkerMemoryPropertiesWithoutSparkEnv()
    {
        // Test when no SparkConf is available (SparkEnv not initialized)
        TestingNativeExecutionProcess process = createTestingNativeExecutionProcess("10", "8", "8GB", null);
        process.updateWorkerMemoryProperties();
        // Verify that values remain unchanged
        Map<String, String> properties = process.getWorkerProperty().getSystemConfig().getAllProperties();
        assertEquals(properties.get(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB), "10");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MEMORY_GB), "8");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE), "8GB");
    }

    @Test
    public void testUpdateWorkerMemoryPropertiesWithOffHeapMemory()
    {
        // Test when spark.memory.offHeap.size is set to a value larger than current system memory
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.memory.offHeap.size", "20g"); // 20GB

        TestingNativeExecutionProcess process = createTestingNativeExecutionProcess("10", "8", "8GB", sparkConf);
        process.updateWorkerMemoryProperties();
        // Verify the updated values
        Map<String, String> properties = process.getWorkerProperty().getSystemConfig().getAllProperties();

        // Expected values:
        // newSystemMemory = 20GB
        // queryMemoryFraction = 8/10 = 0.8
        // newQueryMemory = 20 * 0.8 = 16GB
        assertEquals(properties.get(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB), "20");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MEMORY_GB), "16");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE), "16GB");
    }

    @Test
    public void testUpdateWorkerMemoryPropertiesWithoutOffHeapSetting()
    {
        // Test when spark.memory.offHeap.size is not set
        SparkConf sparkConf = new SparkConf();
        // Don't set spark.memory.offHeap.size

        TestingNativeExecutionProcess process = createTestingNativeExecutionProcess("10", "8", "8GB", sparkConf);
        process.updateWorkerMemoryProperties();

        // Verify that values remain unchanged
        Map<String, String> properties = process.getWorkerProperty().getSystemConfig().getAllProperties();
        assertEquals(properties.get(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB), "10");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MEMORY_GB), "8");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE), "8GB");
    }

    @Test
    public void testUpdateWorkerMemoryPropertiesWithZeroOffHeapMemory()
    {
        // Test when spark.memory.offHeap.size is set to 0
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.memory.offHeap.size", "0b");

        TestingNativeExecutionProcess process = createTestingNativeExecutionProcess("10", "8", "8GB", sparkConf);
        process.updateWorkerMemoryProperties();

        // Verify that values remain unchanged when offHeapMemory is 0
        Map<String, String> properties = process.getWorkerProperty().getSystemConfig().getAllProperties();
        assertEquals(properties.get(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB), "10");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MEMORY_GB), "8");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE), "8GB");
    }

    @Test
    public void testUpdateWorkerMemoryPropertiesWithSmallerOffHeapMemory()
    {
        // Test when spark.memory.offHeap.size is smaller than current system memory
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.memory.offHeap.size", "5g"); // 5GB (smaller than current 10GB)

        TestingNativeExecutionProcess process = createTestingNativeExecutionProcess("10", "8", "8GB", sparkConf);
        process.updateWorkerMemoryProperties();

        // Verify that values remain unchanged when offHeapMemory is smaller than current system memory
        Map<String, String> properties = process.getWorkerProperty().getSystemConfig().getAllProperties();
        assertEquals(properties.get(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB), "10");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MEMORY_GB), "8");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE), "8GB");
    }

    @Test
    public void testUpdateWorkerMemoryPropertiesWithDifferentQueryMemoryFraction()
    {
        // Test with different query memory to system memory ratio
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.memory.offHeap.size", "30g"); // 30GB

        TestingNativeExecutionProcess process = createTestingNativeExecutionProcess("12", "6", "6GB", sparkConf);
        process.updateWorkerMemoryProperties();

        // Verify the updated values
        Map<String, String> properties = process.getWorkerProperty().getSystemConfig().getAllProperties();

        // Expected values:
        // newSystemMemory = 30GB
        // queryMemoryFraction = 6/12 = 0.5
        // newQueryMemory = 30 * 0.5 = 15GB
        assertEquals(properties.get(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB), "30");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MEMORY_GB), "15");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE), "15GB");
    }

    @Test
    public void testUpdateWorkerMemoryPropertiesWithZeroCurrentSystemMemory()
    {
        // Test edge case when current system memory is 0
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.memory.offHeap.size", "20g"); // 20GB

        TestingNativeExecutionProcess process = createTestingNativeExecutionProcess("0", "8", "8GB", sparkConf);
        process.updateWorkerMemoryProperties();

        // Verify that values remain unchanged when current system memory is 0
        Map<String, String> properties = process.getWorkerProperty().getSystemConfig().getAllProperties();
        assertEquals(properties.get(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB), "0");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MEMORY_GB), "8");
        assertEquals(properties.get(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE), "8GB");
    }

    private TestingNativeExecutionProcess createTestingNativeExecutionProcess(
            String systemMemoryGb,
            String queryMemoryGb,
            String queryMaxMemoryPerNode,
            SparkConf sparkConf)
    {
        Map<String, String> systemConfigs = new HashMap<>();
        systemConfigs.put(NativeExecutionSystemConfig.SYSTEM_MEMORY_GB, systemMemoryGb);
        systemConfigs.put(NativeExecutionSystemConfig.QUERY_MEMORY_GB, queryMemoryGb);
        systemConfigs.put(NativeExecutionSystemConfig.QUERY_MAX_MEMORY_PER_NODE, queryMaxMemoryPerNode);

        NativeExecutionSystemConfig systemConfig = new NativeExecutionSystemConfig(systemConfigs);
        PrestoSparkWorkerProperty workerProperty = new PrestoSparkWorkerProperty(
                new NativeExecutionCatalogProperties(ImmutableMap.of()),
                new NativeExecutionNodeConfig(),
                systemConfig);

        Session session = testSessionBuilder().build();

        try {
            return new TestingNativeExecutionProcess(
                    "/bin/echo",
                    "",
                    session,
                    new TestPrestoSparkHttpClient.TestingOkHttpClient(newScheduledThreadPool(4),
                            new TestPrestoSparkHttpClient.TestingResponseManager("test")),
                    newSingleThreadExecutor(),
                    newScheduledThreadPool(4),
                    SERVER_INFO_JSON_CODEC,
                    new Duration(10, TimeUnit.SECONDS),
                    workerProperty,
                    sparkConf);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Testing subclass that allows injection of custom SparkConf for testing
     */
    private static class TestingNativeExecutionProcess
            extends NativeExecutionProcess
    {
        private static final Logger log = Logger.get(TestingNativeExecutionProcess.class);
        private final SparkConf testSparkConf;
        public TestingNativeExecutionProcess(
                String executablePath,
                String programArguments,
                Session session,
                OkHttpClient httpClient,
                java.util.concurrent.Executor executor,
                ScheduledExecutorService scheduledExecutorService,
                JsonCodec<ServerInfo> serverInfoCodec,
                Duration maxErrorDuration,
                PrestoSparkWorkerProperty workerProperty,
                SparkConf testSparkConf)
                throws IOException
        {
            super(executablePath, programArguments, session, httpClient, executor,
                    scheduledExecutorService, serverInfoCodec, maxErrorDuration, workerProperty);
            this.testSparkConf = testSparkConf;
        }

        @Override
        protected SparkConf getSparkConf()
        {
            return testSparkConf;
        }

        public PrestoSparkWorkerProperty getWorkerProperty()
        {
            return super.getWorkerProperty();
        }

        @Override
        protected void updateWorkerMemoryProperties()
        {
            super.updateWorkerMemoryProperties();
        }
    }

    private NativeExecutionProcessFactory createNativeExecutionProcessFactory()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        ScheduledExecutorService errorScheduler = newScheduledThreadPool(4);
        PrestoSparkWorkerProperty workerProperty = new PrestoSparkWorkerProperty(
                new NativeExecutionCatalogProperties(ImmutableMap.of()),
                new NativeExecutionNodeConfig(),
                new NativeExecutionSystemConfig(ImmutableMap.of()));
        NativeExecutionProcessFactory factory = new NativeExecutionProcessFactory(
                new TestPrestoSparkHttpClient.TestingOkHttpClient(
                        errorScheduler,
                        new TestPrestoSparkHttpClient.TestingResponseManager(taskId.toString(), new TestPrestoSparkHttpClient.FailureRetryResponseManager(5))),
                newSingleThreadExecutor(),
                errorScheduler,
                SERVER_INFO_JSON_CODEC,
                workerProperty,
                new FeaturesConfig().setNativeExecutionExecutablePath("/bin/echo"));
        return factory;
    }
}
