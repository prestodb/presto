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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.RequestStats;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestRetryFunctionality
{
    private ScheduledExecutorService scheduler;
    private ImplHttpResourceManagerClient client;
    private FailingHttpClient failingHttpClient;
    private InternalNodeManager nodeManager;

    @BeforeMethod
    public void setUp()
    {
        scheduler = newSingleThreadScheduledExecutor();
        failingHttpClient = new FailingHttpClient();

        InMemoryNodeManager manager = new InMemoryNodeManager();
        manager.addNode(new ConnectorId("test"),
                    new InternalNode("rm", URI.create("http://localhost:8080/"), new NodeVersion("1"), false, true, false, false));
        nodeManager = manager;

        client = new ImplHttpResourceManagerClient(
                failingHttpClient,
                scheduler,
                JsonCodec.jsonCodec(NodeStatus.class),
                JsonCodec.jsonCodec(BasicQueryInfo.class),
                JsonCodec.listJsonCodec(ResourceGroupRuntimeInfo.class),
                JsonCodec.mapJsonCodec(MemoryPoolId.class, ClusterMemoryPoolInfo.class),
                nodeManager);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        scheduler.shutdownNow();
    }

    @Test
    public void testRetryOnIOException()
    {
        failingHttpClient.setFailuresBeforeSuccess(2);
        failingHttpClient.setExceptionToThrow(new IOException("Simulated network error"));

        NodeStatus nodeStatus = createTestNodeStatus("test-node");
        client.nodeHeartbeat(Optional.empty(), nodeStatus);

        assertEquals(failingHttpClient.getAttemptCount(), 3, "Expected 3 attempts (2 failures + 1 success)");
    }

    @Test
    public void testRetryOnSocketTimeout()
    {
        failingHttpClient.setFailuresBeforeSuccess(1);
        failingHttpClient.setExceptionToThrow(new SocketTimeoutException("Connection timeout"));

        NodeStatus nodeStatus = createTestNodeStatus("test-node");
        client.nodeHeartbeat(Optional.empty(), nodeStatus);

        assertEquals(failingHttpClient.getAttemptCount(), 2, "Expected 2 attempts (1 timeout + 1 success)");
    }

    @Test
    public void testMaxRetriesExceeded()
    {
        failingHttpClient.setFailuresBeforeSuccess(10);
        failingHttpClient.setExceptionToThrow(new IOException("Persistent network error"));

        NodeStatus nodeStatus = createTestNodeStatus("test-node");

        try {
            client.nodeHeartbeat(Optional.empty(), nodeStatus);
            fail("Expected ResourceManagerException to be thrown");
        }
        catch (Exception e) {
            e.printStackTrace();
            assertTrue(failingHttpClient.getAttemptCount() >= 6, "Expected at least 6 attempts, got: " + failingHttpClient.getAttemptCount());
        }
    }

    @Test
    public void testNoRetryOnNonIOException()
    {
        failingHttpClient.setFailuresBeforeSuccess(10);
        failingHttpClient.setExceptionToThrow(new IllegalArgumentException("Invalid argument"));

        NodeStatus nodeStatus = createTestNodeStatus("test-node");
        try {
            client.nodeHeartbeat(Optional.empty(), nodeStatus);
            fail("Expected exception to be thrown");
        }
        catch (Exception e) {
            assertEquals(failingHttpClient.getAttemptCount(), 1, "Should not retry on non-IO exceptions");
        }
    }

    @Test
    public void testImmediateSuccess()
    {
        failingHttpClient.setFailuresBeforeSuccess(0);

        NodeStatus nodeStatus = createTestNodeStatus("test-node");
        client.nodeHeartbeat(Optional.empty(), nodeStatus);

        assertEquals(failingHttpClient.getAttemptCount(), 1, "Expected 1 attempt (immediate success)");
    }

    private static NodeStatus createTestNodeStatus(String nodeId)
    {
        return new NodeStatus(
                nodeId,
                new NodeVersion("1"),
                "test",
                true,
                Duration.valueOf("1ms"),
                "loc",
                "loc",
                new MemoryInfo(new DataSize(1, MEGABYTE), ImmutableMap.of()),
                1, 1, 1, 1, 1, 1);
    }

    private static class FailingHttpClient
            implements HttpClient
    {
        private final AtomicInteger attemptCount = new AtomicInteger(0);
        private volatile int failuresBeforeSuccess;
        private volatile Exception exceptionToThrow = new IOException("Default error");

        public void setFailuresBeforeSuccess(int failures)
        {
            this.failuresBeforeSuccess = failures;
            this.attemptCount.set(0);
        }

        public void setExceptionToThrow(Exception exception)
        {
            this.exceptionToThrow = exception;
        }

        public int getAttemptCount()
        {
            return attemptCount.get();
        }

        @Override
        public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
                throws E
        {
            int attempt = attemptCount.incrementAndGet();

            if (attempt <= failuresBeforeSuccess) {
                throw (E) exceptionToThrow;
            }

            return null;
        }

        @Override
        public <T, E extends Exception> HttpClient.HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
        {
            return null;
        }

        @Override
        public RequestStats getStats()
        {
            return null;
        }

        @Override
        public long getMaxContentLength()
        {
            return 0;
        }

        @Override
        public void close() {}

        @Override
        public boolean isClosed()
        {
            return false;
        }
    }
}
