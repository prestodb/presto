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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.QueryTracker.TrackedQuery;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.units.Duration.succinctDuration;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestQueryTrackerQueuedTime
{
    private ScheduledExecutorService executor;
    private QueryTracker<MockTrackedQuery> queryTracker;

    @BeforeMethod
    public void setUp()
    {
        executor = newSingleThreadScheduledExecutor();
        QueryManagerConfig config = new QueryManagerConfig();
        queryTracker = new QueryTracker<>(config, executor, Optional.empty());
        queryTracker.start();
    }

    @AfterMethod
    public void tearDown()
    {
        if (queryTracker != null) {
            queryTracker.stop();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testQueryExceedsQueuedTimeLimit()
            throws Exception
    {
        // Create a session with 1 second queued time limit
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(SystemSessionProperties.QUERY_MAX_QUEUED_TIME, "1s")
                .build();

        AtomicReference<PrestoException> failureException = new AtomicReference<>();
        AtomicBoolean queryFailed = new AtomicBoolean(false);

        // Create a mock query that has been queued for 2 seconds (exceeds limit)
        long currentTime = System.currentTimeMillis();
        MockTrackedQuery query = new MockTrackedQuery(
                new QueryId("test_query_1"),
                session,
                currentTime - 2000, // Created 2 seconds ago
                0, // Not started execution yet
                currentTime,
                failureException,
                queryFailed);

        queryTracker.addQuery(query);

        // Manually trigger time limit enforcement
        queryTracker.enforceTimeLimits();

        // Verify the query was failed due to exceeding queued time limit
        assertTrue(queryFailed.get(), "Query should have been failed");
        assertNotNull(failureException.get(), "Failure exception should be set");
        assertEquals(failureException.get().getErrorCode(), EXCEEDED_TIME_LIMIT.toErrorCode());
        assertTrue(failureException.get().getMessage().contains("Query exceeded maximum queued time limit"));
    }

    @Test
    public void testQueryWithinQueuedTimeLimit()
            throws Exception
    {
        // Create a session with 5 second queued time limit
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(SystemSessionProperties.QUERY_MAX_QUEUED_TIME, "5s")
                .build();

        AtomicReference<PrestoException> failureException = new AtomicReference<>();
        AtomicBoolean queryFailed = new AtomicBoolean(false);

        // Create a mock query that has been queued for 1 second (within limit)
        long currentTime = System.currentTimeMillis();
        MockTrackedQuery query = new MockTrackedQuery(
                new QueryId("test_query_2"),
                session,
                currentTime - 1000, // Created 1 second ago
                0, // Not started execution yet
                currentTime,
                failureException,
                queryFailed);

        queryTracker.addQuery(query);

        // Manually trigger time limit enforcement
        queryTracker.enforceTimeLimits();

        // Verify the query was not failed
        assertFalse(queryFailed.get(), "Query should not have been failed");
    }

    @Test
    public void testQueryStartedExecutionQueuedTimeCalculation()
            throws Exception
    {
        // Create a session with 1 second queued time limit
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(SystemSessionProperties.QUERY_MAX_QUEUED_TIME, "1s")
                .build();

        AtomicReference<PrestoException> failureException = new AtomicReference<>();
        AtomicBoolean queryFailed = new AtomicBoolean(false);

        // Create a mock query that was queued for 2 seconds but started execution
        long currentTime = System.currentTimeMillis();
        MockTrackedQuery query = new MockTrackedQuery(
                new QueryId("test_query_3"),
                session,
                currentTime - 3000, // Created 3 seconds ago
                currentTime - 1000, // Started execution 1 second ago (queued for 2 seconds)
                currentTime,
                failureException,
                queryFailed);

        queryTracker.addQuery(query);

        // Manually trigger time limit enforcement
        queryTracker.enforceTimeLimits();

        // Verify the query was failed because it was queued for 2 seconds (exceeds 1s limit)
        assertTrue(queryFailed.get(), "Query should have been failed");
        assertNotNull(failureException.get(), "Failure exception should be set");
        assertEquals(failureException.get().getErrorCode(), EXCEEDED_TIME_LIMIT.toErrorCode());
        assertTrue(failureException.get().getMessage().contains("Query exceeded maximum queued time limit"));
    }

    @Test
    public void testQueryStartedExecutionWithinQueuedTimeLimit()
            throws Exception
    {
        // Create a session with 5 second queued time limit
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(SystemSessionProperties.QUERY_MAX_QUEUED_TIME, "5s")
                .build();

        AtomicReference<PrestoException> failureException = new AtomicReference<>();
        AtomicBoolean queryFailed = new AtomicBoolean(false);

        // Create a mock query that was queued for 1 second and started execution
        long currentTime = System.currentTimeMillis();
        MockTrackedQuery query = new MockTrackedQuery(
                new QueryId("test_query_4"),
                session,
                currentTime - 2000, // Created 2 seconds ago
                currentTime - 1000, // Started execution 1 second ago (queued for 1 second)
                currentTime,
                failureException,
                queryFailed);

        queryTracker.addQuery(query);

        // Manually trigger time limit enforcement
        queryTracker.enforceTimeLimits();

        // Verify the query was not failed
        assertFalse(queryFailed.get(), "Query should not have been failed");
    }

    @Test
    public void testCompletedQueryNotChecked()
            throws Exception
    {
        // Create a session with 1 second queued time limit
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(SystemSessionProperties.QUERY_MAX_QUEUED_TIME, "1s")
                .build();

        AtomicReference<PrestoException> failureException = new AtomicReference<>();
        AtomicBoolean queryFailed = new AtomicBoolean(false);

        // Create a mock query that is already completed
        long currentTime = System.currentTimeMillis();
        MockTrackedQuery query = new MockTrackedQuery(
                new QueryId("test_query_5"),
                session,
                currentTime - 5000, // Created 5 seconds ago
                0, // Not started execution yet
                currentTime,
                failureException,
                queryFailed);
        query.setDone(true); // Mark as completed

        queryTracker.addQuery(query);

        // Manually trigger time limit enforcement
        queryTracker.enforceTimeLimits();

        // Verify the completed query was not failed
        assertFalse(queryFailed.get(), "Completed query should not be checked for time limits");
    }

    private static class MockTrackedQuery
            implements TrackedQuery
    {
        private final QueryId queryId;
        private final Session session;
        private final long createTimeInMillis;
        private final long executionStartTimeInMillis;
        private final long lastHeartbeatInMillis;
        private final AtomicReference<PrestoException> failureException;
        private final AtomicBoolean queryFailed;
        private boolean done;

        public MockTrackedQuery(
                QueryId queryId,
                Session session,
                long createTimeInMillis,
                long executionStartTimeInMillis,
                long lastHeartbeatInMillis,
                AtomicReference<PrestoException> failureException,
                AtomicBoolean queryFailed)
        {
            this.queryId = queryId;
            this.session = session;
            this.createTimeInMillis = createTimeInMillis;
            this.executionStartTimeInMillis = executionStartTimeInMillis;
            this.lastHeartbeatInMillis = lastHeartbeatInMillis;
            this.failureException = failureException;
            this.queryFailed = queryFailed;
        }

        public void setDone(boolean done)
        {
            this.done = done;
        }

        @Override
        public QueryId getQueryId()
        {
            return queryId;
        }

        @Override
        public boolean isDone()
        {
            return done;
        }

        @Override
        public Session getSession()
        {
            return session;
        }

        @Override
        public long getCreateTimeInMillis()
        {
            return createTimeInMillis;
        }

        @Override
        public Duration getQueuedTime()
        {
            long queuedTimeInMillis;
            if (executionStartTimeInMillis > 0) {
                queuedTimeInMillis = executionStartTimeInMillis - createTimeInMillis;
            }
            else {
                queuedTimeInMillis = System.currentTimeMillis() - createTimeInMillis;
            }
            return succinctDuration(queuedTimeInMillis, MILLISECONDS);
        }

        @Override
        public long getExecutionStartTimeInMillis()
        {
            return executionStartTimeInMillis;
        }

        @Override
        public long getLastHeartbeatInMillis()
        {
            return lastHeartbeatInMillis;
        }

        @Override
        public long getEndTimeInMillis()
        {
            return done ? System.currentTimeMillis() : 0;
        }

        @Override
        public Optional<ResourceGroupQueryLimits> getResourceGroupQueryLimits()
        {
            return Optional.empty();
        }

        @Override
        public void fail(Throwable cause)
        {
            if (cause instanceof PrestoException) {
                failureException.set((PrestoException) cause);
            }
            queryFailed.set(true);
            done = true;
        }

        @Override
        public void pruneExpiredQueryInfo()
        {
            // No-op for test
        }

        @Override
        public void pruneFinishedQueryInfo()
        {
            // No-op for test
        }
    }
}
