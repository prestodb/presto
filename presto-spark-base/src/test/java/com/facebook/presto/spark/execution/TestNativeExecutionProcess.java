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
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFatalException;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcess;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcessFactory;
import com.facebook.presto.spark.execution.property.NativeExecutionConnectorConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionNodeConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionVeloxConfig;
import com.facebook.presto.spark.execution.property.PrestoSparkWorkerProperty;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilder;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_EXECUTABLE_PATH;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestNativeExecutionProcess
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_JSON_CODEC = JsonCodec.jsonCodec(ServerInfo.class);
    private static final URI BASE_URI = uriBuilder()
            .scheme("http")
            .host("localhost")
            .port(8080)
            .build();

    @Test
    public void testNativeProcessIsAlive()
    {
        Session session = testSessionBuilder().build();
        NativeExecutionProcessFactory factory = createNativeExecutionProcessFactory();
        NativeExecutionProcess process = factory.getNativeExecutionProcess(session, BASE_URI);
        // Simulate the process is closed (crashed)
        process.close();
        assertFalse(process.isAlive());
    }

    @Test
    public void testNativeProcessRelaunch()
    {
        Session session = testSessionBuilder().build();
        NativeExecutionProcessFactory factory = createNativeExecutionProcessFactory();
        NativeExecutionProcess process = factory.getNativeExecutionProcess(session, BASE_URI);
        // Simulate the process is closed (crashed)
        process.close();
        assertFalse(process.isAlive());
        NativeExecutionProcess process2 = factory.getNativeExecutionProcess(session, BASE_URI);
        // Expecting the factory re-created a new process object so that the process and process2
        // should be two different objects
        assertNotSame(process2, process);
    }

    @Test
    public void testNativeProcessShutdown()
    {
        Session session = testSessionBuilder().setSystemProperty(NATIVE_EXECUTION_EXECUTABLE_PATH, "/bin/echo").build();
        NativeExecutionProcessFactory factory = createNativeExecutionProcessFactory();
        // Set the maxRetryDuration to 0 ms to allow the RequestErrorTracker failing immediately
        NativeExecutionProcess process = factory.createNativeExecutionProcess(session, BASE_URI, new Duration(0, TimeUnit.MILLISECONDS));
        Throwable exception = expectThrows(PrestoSparkFatalException.class, process::start);
        assertTrue(exception.getMessage().contains("Native process launch failed with multiple retries"));
        assertFalse(process.isAlive());
    }

    private NativeExecutionProcessFactory createNativeExecutionProcessFactory()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        ScheduledExecutorService errorScheduler = newScheduledThreadPool(4);
        PrestoSparkWorkerProperty workerProperty = new PrestoSparkWorkerProperty(
                new NativeExecutionConnectorConfig(),
                new NativeExecutionNodeConfig(),
                new NativeExecutionSystemConfig(),
                new NativeExecutionVeloxConfig());
        NativeExecutionProcessFactory factory = new NativeExecutionProcessFactory(
                new TestPrestoSparkHttpClient.TestingHttpClient(
                        new TestPrestoSparkHttpClient.TestingResponseManager(taskId.toString(), new TestPrestoSparkHttpClient.FailureRetryResponseManager(5))),
                newSingleThreadExecutor(),
                errorScheduler,
                SERVER_INFO_JSON_CODEC,
                workerProperty);
        return factory;
    }
}
