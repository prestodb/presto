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
package com.facebook.presto.execution;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConnectorId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.concurrent.MoreFutures.addExceptionCallback;
import static com.facebook.airlift.concurrent.MoreFutures.addSuccessCallback;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClusterSizeMonitor
{
    public static final ConnectorId CONNECTOR_ID = new ConnectorId("dummy");
    public static final int DESIRED_WORKER_COUNT = 10;
    public static final int DESIRED_COORDINATOR_COUNT = 3;
    public static final int DESIRED_COORDINATOR_COUNT_ACTIVE = 2;
    public static final int DESIRED_RESOURCE_MANAGER_COUNT_ACTIVE = 1;
    public static final int DESIRED_WORKER_COUNT_ACTIVE = 10;

    private InMemoryNodeManager nodeManager;
    private ClusterSizeMonitor monitor;
    private CountDownLatch minWorkersLatch;
    private AtomicInteger numWorkers;
    private AtomicInteger numCoordinators;
    private AtomicInteger numResourceManagers;
    private AtomicBoolean workersTimeout;

    @BeforeMethod
    public void setUp()
    {
        numWorkers = new AtomicInteger(0);
        numCoordinators = new AtomicInteger(0);
        numResourceManagers = new AtomicInteger(0);
        workersTimeout = new AtomicBoolean();

        nodeManager = new InMemoryNodeManager();
        monitor = new ClusterSizeMonitor(
                nodeManager,
                false,
                DESIRED_WORKER_COUNT,
                DESIRED_WORKER_COUNT_ACTIVE,
                new Duration(4, SECONDS),
                DESIRED_COORDINATOR_COUNT,
                DESIRED_COORDINATOR_COUNT_ACTIVE,
                new Duration(4, SECONDS),
                DESIRED_RESOURCE_MANAGER_COUNT_ACTIVE);

        minWorkersLatch = new CountDownLatch(1);

        monitor.start();
    }

    @AfterMethod
    public void tearDown()
    {
        monitor.stop();
    }

    @Test(timeOut = 60_000)
    public void testWaitForMinimumWorkers()
            throws InterruptedException
    {
        ListenableFuture<?> workersFuture = waitForMinimumWorkers();

        for (int i = numWorkers.get() + 1; i < DESIRED_WORKER_COUNT - 1; i++) {
            assertFalse(workersTimeout.get());
            addWorker(nodeManager);
        }
        assertFalse(monitor.hasRequiredWorkers());
        assertFalse(workersTimeout.get());
        assertEquals(minWorkersLatch.getCount(), 1);
        addWorker(nodeManager);
        minWorkersLatch.await(1, SECONDS);
        assertTrue(workersFuture.isDone());
        assertFalse(workersTimeout.get());
        assertTrue(monitor.hasRequiredWorkers());
    }

    @Test(timeOut = 10_000)
    public void testTimeoutWaitingForWorkers()
            throws InterruptedException
    {
        waitForMinimumWorkers();

        assertFalse(workersTimeout.get());
        addWorker(nodeManager);
        assertFalse(workersTimeout.get());
        assertEquals(minWorkersLatch.getCount(), 1);

        Thread.sleep(SECONDS.toMillis(5));

        assertTrue(workersTimeout.get());
        assertEquals(minWorkersLatch.getCount(), 0);
    }

    @Test
    public void testHasRequiredResourceManagers()
            throws InterruptedException
    {
        assertFalse(monitor.hasRequiredResourceManagers());
        for (int i = numResourceManagers.get(); i < DESIRED_RESOURCE_MANAGER_COUNT_ACTIVE; i++) {
            addResourceManager(nodeManager);
        }
        assertTrue(monitor.hasRequiredResourceManagers());
    }

    @Test
    public void testHasRequiredCoordinators()
            throws InterruptedException
    {
        assertFalse(monitor.hasRequiredCoordinators());
        for (int i = numResourceManagers.get(); i < DESIRED_COORDINATOR_COUNT_ACTIVE; i++) {
            addCoordinator(nodeManager);
        }
        assertTrue(monitor.hasRequiredCoordinators());
    }

    private ListenableFuture<?> waitForMinimumWorkers()
    {
        ListenableFuture<?> workersFuture = monitor.waitForMinimumWorkers();
        addSuccessCallback(workersFuture, () -> {
            assertFalse(workersTimeout.get());
            minWorkersLatch.countDown();
        });
        addExceptionCallback(workersFuture, () -> {
            assertTrue(workersTimeout.compareAndSet(false, true));
            minWorkersLatch.countDown();
        });
        return workersFuture;
    }

    private void addWorker(InMemoryNodeManager nodeManager)
    {
        String identifier = "worker/" + numWorkers.incrementAndGet();
        nodeManager.addNode(CONNECTOR_ID, new InternalNode(identifier, URI.create("localhost/" + identifier), new NodeVersion("1"), false));
    }

    private void addCoordinator(InMemoryNodeManager nodeManager)
    {
        String identifier = "coordinator/" + numCoordinators.incrementAndGet();
        nodeManager.addNode(CONNECTOR_ID, new InternalNode(identifier, URI.create("localhost/" + identifier), new NodeVersion("1"), true));
    }

    private void addResourceManager(InMemoryNodeManager nodeManager)
    {
        String identifier = "resource_manager/" + numResourceManagers.incrementAndGet();
        nodeManager.addNode(CONNECTOR_ID, new InternalNode(identifier, URI.create("localhost/" + identifier), new NodeVersion("1"), false, true));
    }
}
