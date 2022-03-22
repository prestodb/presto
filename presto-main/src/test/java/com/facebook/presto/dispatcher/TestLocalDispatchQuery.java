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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.Session;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.event.QueryMonitorConfig;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.MockQueryExecution;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.resourceGroups.QueryQueueFullException;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.prerequisites.QueryPrerequisites;
import com.facebook.presto.spi.prerequisites.QueryPrerequisitesContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.NodeVersion.UNKNOWN;
import static com.facebook.presto.execution.QueryState.DISPATCHING;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_PREREQUISITES;
import static com.facebook.presto.execution.TaskTestUtils.createQueryStateMachine;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_TASK;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_QUEUE_FULL;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestLocalDispatchQuery
{
    private static final QueryPrerequisites QUERY_PREREQUISITES = new DefaultQueryPrerequisites();
    private final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testSimpleExecutionCreationFailure()
    {
        CountingEventListener eventListener = new CountingEventListener();

        LocalDispatchQuery query = new LocalDispatchQuery(
                createStateMachine(),
                createQueryMonitor(eventListener),
                immediateFailedFuture(new IllegalStateException("abc")),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {},
                false,
                QUERY_PREREQUISITES);

        assertEquals(query.getBasicQueryInfo().getState(), FAILED);
        assertEquals(query.getBasicQueryInfo().getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertTrue(eventListener.getQueryCompletedEvent().isPresent());
        assertTrue(eventListener.getQueryCompletedEvent().get().getFailureInfo().isPresent());
        assertEquals(eventListener.getQueryCompletedEvent().get().getFailureInfo().get().getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
    }

    @Test
    public void testQueryQueuedExceptionBeforeDispatch()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();

        SettableFuture<QueryExecution> queryExecutionFuture = SettableFuture.create();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                queryExecutionFuture,
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {
                    throw new QueryQueueFullException(new ResourceGroupId("global"));
                },
                execution -> {},
                false,
                QUERY_PREREQUISITES);

        query.startWaitingForPrerequisites();
        queryExecutionFuture.setException(new IllegalStateException("abc"));

        assertEquals(query.getBasicQueryInfo().getState(), FAILED);
        assertEquals(query.getBasicQueryInfo().getErrorCode(), QUERY_QUEUE_FULL.toErrorCode());
        assertTrue(eventListener.getQueryCompletedEvent().isPresent());
        assertTrue(eventListener.getQueryCompletedEvent().get().getFailureInfo().isPresent());
        assertEquals(eventListener.getQueryCompletedEvent().get().getFailureInfo().get().getErrorCode(), QUERY_QUEUE_FULL.toErrorCode());
    }

    @Test
    public void testErrorInPrerequisitesFuture()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(null),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {
                    throw new AccessDeniedException("sdf");
                },
                false,
                (queryId, context, warningCollector) -> {
                    CompletableFuture<?> future = new CompletableFuture<>();
                    future.completeExceptionally(new PrestoException(ABANDONED_TASK, "something went wrong"));
                    return future;
                });

        assertEquals(query.getBasicQueryInfo().getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());

        query.startWaitingForPrerequisites();
        assertEquals(query.getBasicQueryInfo().getState(), FAILED);
        assertEquals(query.getBasicQueryInfo().getErrorCode(), ABANDONED_TASK.toErrorCode());
        assertTrue(eventListener.getQueryCompletedEvent().isPresent());
        assertTrue(eventListener.getQueryCompletedEvent().get().getFailureInfo().isPresent());
        assertEquals(eventListener.getQueryCompletedEvent().get().getFailureInfo().get().getErrorCode(), ABANDONED_TASK.toErrorCode());
    }

    @Test
    public void testErrorInPrerequisitesSubmission()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(null),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {
                    throw new AccessDeniedException("sdf");
                },
                false,
                (queryId, context, warningCollector) -> {
                    throw new PrestoException(ABANDONED_QUERY, "something went wrong");
                });

        assertEquals(query.getBasicQueryInfo().getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());

        try {
            query.startWaitingForPrerequisites();
            fail("Exception should be thrown");
        }
        catch (Throwable t) {
            assertEquals(query.getBasicQueryInfo().getState(), FAILED);
            assertEquals(query.getBasicQueryInfo().getErrorCode(), ABANDONED_QUERY.toErrorCode());
            assertTrue(eventListener.getQueryCompletedEvent().isPresent());
            assertTrue(eventListener.getQueryCompletedEvent().get().getFailureInfo().isPresent());
            assertEquals(eventListener.getQueryCompletedEvent().get().getFailureInfo().get().getErrorCode(), ABANDONED_QUERY.toErrorCode());
        }
    }

    @Test
    public void testPrerequisitesQueryFinishedCalled()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();
        CompletableFuture<?> prequisitesFuture = new CompletableFuture<>();
        AtomicBoolean queryFinishedCalled = new AtomicBoolean();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(null),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {},
                false,
                new QueryPrerequisites() {
                    @Override
                    public CompletableFuture<?> waitForPrerequisites(QueryId queryId, QueryPrerequisitesContext context, WarningCollector warningCollector)
                    {
                        return prequisitesFuture;
                    }

                    @Override
                    public void queryFinished(QueryId queryId)
                    {
                        queryFinishedCalled.set(true);
                    }
                });

        assertEquals(query.getBasicQueryInfo().getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());

        query.startWaitingForPrerequisites();
        prequisitesFuture.complete(null);
        query.fail(new PrestoException(ABANDONED_QUERY, "foo"));

        assertTrue(queryFinishedCalled.get());
    }

    @Test
    public void testPrerequisiteFutureCancellationWhenQueryCancelled()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();
        CompletableFuture<?> prequisitesFuture = new CompletableFuture<>();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(null),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {},
                false,
                (queryId, context, warningCollector) -> prequisitesFuture);

        assertEquals(query.getBasicQueryInfo().getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());

        query.startWaitingForPrerequisites();
        query.fail(new PrestoException(ABANDONED_QUERY, "foo"));

        assertTrue(prequisitesFuture.isCancelled());
    }

    @Test
    public void testQueryQueueSubmission()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();
        AtomicBoolean queryQueuerCalled = new AtomicBoolean();
        CompletableFuture<?> prerequisitesFuture = new CompletableFuture<>();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(null),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {
                    queryQueuerCalled.compareAndSet(false, true);
                },
                execution -> {},
                false,
                (queryId, context, warningCollector) -> prerequisitesFuture);

        assertEquals(stateMachine.getBasicQueryInfo(Optional.empty()).getState(), WAITING_FOR_PREREQUISITES);
        query.startWaitingForPrerequisites();

        assertEquals(stateMachine.getBasicQueryInfo(Optional.empty()).getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(queryQueuerCalled.get());

        prerequisitesFuture.complete(null);

        assertEquals(stateMachine.getBasicQueryInfo(Optional.empty()).getState(), QUEUED);
        assertTrue(queryQueuerCalled.get());
    }

    @Test
    public void testErrorInQuerySubmitter()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(new MockQueryExecution()),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {
                    throw new AccessDeniedException("sdf");
                },
                false,
                QUERY_PREREQUISITES);

        assertEquals(query.getBasicQueryInfo().getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());

        query.startWaitingForResources();

        assertEquals(query.getBasicQueryInfo().getState(), FAILED);
        assertEquals(query.getBasicQueryInfo().getErrorCode(), PERMISSION_DENIED.toErrorCode());
        assertTrue(eventListener.getQueryCompletedEvent().isPresent());
        assertTrue(eventListener.getQueryCompletedEvent().get().getFailureInfo().isPresent());
        assertEquals(eventListener.getQueryCompletedEvent().get().getFailureInfo().get().getErrorCode(), PERMISSION_DENIED.toErrorCode());
    }

    @Test
    public void testTimeOutWaitingForClusterResources()
            throws Exception
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(null),
                createClusterSizeMonitor(1),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {},
                false,
                QUERY_PREREQUISITES);

        assertEquals(query.getBasicQueryInfo().getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());

        query.startWaitingForResources();

        Thread.sleep(300); // Sleep long enough to ensure resource exhaustion error

        assertEquals(query.getBasicQueryInfo().getState(), FAILED);
        assertEquals(query.getBasicQueryInfo().getErrorCode(), GENERIC_INSUFFICIENT_RESOURCES.toErrorCode());
        assertTrue(eventListener.getQueryCompletedEvent().isPresent());
        assertTrue(eventListener.getQueryCompletedEvent().get().getFailureInfo().isPresent());
        assertEquals(eventListener.getQueryCompletedEvent().get().getFailureInfo().get().getErrorCode(), GENERIC_INSUFFICIENT_RESOURCES.toErrorCode());
    }

    @Test
    public void testQueryCancellation()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(null),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {},
                false,
                QUERY_PREREQUISITES);

        assertEquals(query.getBasicQueryInfo().getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());

        query.cancel();

        assertEquals(query.getBasicQueryInfo().getState(), FAILED);
        assertEquals(query.getBasicQueryInfo().getErrorCode(), USER_CANCELED.toErrorCode());
        assertTrue(eventListener.getQueryCompletedEvent().isPresent());
        assertTrue(eventListener.getQueryCompletedEvent().get().getFailureInfo().isPresent());
        assertEquals(eventListener.getQueryCompletedEvent().get().getFailureInfo().get().getErrorCode(), USER_CANCELED.toErrorCode());
    }

    @Test
    public void testQueryDispatched()
    {
        QueryStateMachine stateMachine = createStateMachine();
        CountingEventListener eventListener = new CountingEventListener();

        LocalDispatchQuery query = new LocalDispatchQuery(
                stateMachine,
                createQueryMonitor(eventListener),
                immediateFuture(new MockQueryExecution()),
                createClusterSizeMonitor(0),
                directExecutor(),
                dispatchQuery -> {},
                execution -> {},
                false,
                QUERY_PREREQUISITES);

        assertEquals(query.getBasicQueryInfo().getState(), WAITING_FOR_PREREQUISITES);
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());

        query.startWaitingForResources();

        assertEquals(query.getBasicQueryInfo().getState(), DISPATCHING);
        assertNull(query.getBasicQueryInfo().getErrorCode());
        assertFalse(eventListener.getQueryCompletedEvent().isPresent());
    }

    private ClusterSizeMonitor createClusterSizeMonitor(int minimumNodes)
    {
        return new ClusterSizeMonitor(new InMemoryNodeManager(), true, minimumNodes, minimumNodes, new Duration(10, MILLISECONDS), 1, 1, new Duration(1, SECONDS), 0);
    }

    private QueryMonitor createQueryMonitor(CountingEventListener eventListener)
    {
        EventListenerManager eventListenerManager = createEventListenerManager(eventListener);
        return new QueryMonitor(
                jsonCodec(StageInfo.class),
                jsonCodec(ExecutionFailureInfo.class),
                jsonCodec(OperatorInfo.class),
                eventListenerManager,
                new NodeInfo("test"),
                UNKNOWN,
                new SessionPropertyManager(),
                metadata,
                new QueryMonitorConfig());
    }

    private EventListenerManager createEventListenerManager(CountingEventListener countingEventListener)
    {
        EventListenerManager eventListenerManager = new EventListenerManager();
        eventListenerManager.addEventListenerFactory(new TestEventListenerFactory(countingEventListener));
        eventListenerManager.loadConfiguredEventListener(ImmutableMap.of("event-listener.name", TestEventListenerFactory.NAME));
        return eventListenerManager;
    }

    private QueryStateMachine createStateMachine()
    {
        TransactionManager transactionManager = createTestTransactionManager();
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        return createQueryStateMachine("COMMIT", session, true, transactionManager, directExecutor(), metadata);
    }

    private static class TestEventListenerFactory
            implements EventListenerFactory
    {
        public static final String NAME = "name";

        private final CountingEventListener countingEventListener;

        public TestEventListenerFactory(CountingEventListener countingEventListener)
        {
            this.countingEventListener = requireNonNull(countingEventListener, "countingEventListener is null");
        }

        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return countingEventListener;
        }
    }

    private static class CountingEventListener
            implements EventListener
    {
        private final AtomicReference<QueryCompletedEvent> queryCompletedEvent = new AtomicReference<>();

        @Override
        public void queryCreated(QueryCreatedEvent queryCreatedEvent)
        {
            fail("Query creation events should not be created in this test");
        }

        @Override
        public void queryCompleted(QueryCompletedEvent event)
        {
            assertTrue(queryCompletedEvent.compareAndSet(null, requireNonNull(event, "event is null")), "Duplicate completion event sent");
        }

        @Override
        public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
        {
            fail("splitCompleted should never be called");
        }

        public Optional<QueryCompletedEvent> getQueryCompletedEvent()
        {
            return Optional.ofNullable(queryCompletedEvent.get());
        }
    }
}
