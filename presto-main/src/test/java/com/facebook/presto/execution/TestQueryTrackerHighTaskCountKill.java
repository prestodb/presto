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

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.spi.StandardErrorCode.QUERY_HAS_TOO_MANY_STAGES;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestQueryTrackerHighTaskCountKill
{
    @Test
    public void testMultipleQueriesKilledDueToTaskCount()
    {
        QueryManagerConfig config = new QueryManagerConfig()
                .setMaxQueryRunningTaskCount(100)
                .setMaxTotalRunningTaskCountToKillQuery(200);
        ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor();
        try {
            QueryTracker<MockQueryExecution> queryTracker = new QueryTracker<>(config, scheduledExecutorService);
            MockQueryExecution smallQuery1 = MockQueryExecution.withRunningTaskCount(50);
            MockQueryExecution largeQueryButNotKilled = MockQueryExecution.withRunningTaskCount(101);
            MockQueryExecution largeQueryToBeKilled1 = MockQueryExecution.withRunningTaskCount(200);
            MockQueryExecution largeQueryToBeKilled2 = MockQueryExecution.withRunningTaskCount(250);

            queryTracker.addQuery(smallQuery1);
            queryTracker.addQuery(largeQueryButNotKilled);
            queryTracker.addQuery(largeQueryToBeKilled1);
            queryTracker.addQuery(largeQueryToBeKilled2);

            queryTracker.enforceTaskLimits();

            assertFalse(smallQuery1.getFailureReason().isPresent(), "small query should not be killed");
            assertFalse(
                    largeQueryButNotKilled.getFailureReason().isPresent(),
                    "query exceeds per query limit, but not killed since not heaviest and cluster can get into better state");
            assertTrue(largeQueryToBeKilled1.getFailureReason().isPresent(), "Query should be killed");
            Throwable failureReason1 = largeQueryToBeKilled1.getFailureReason().get();
            assertTrue(failureReason1 instanceof PrestoException);
            assertEquals(((PrestoException) failureReason1).getErrorCode(), QUERY_HAS_TOO_MANY_STAGES.toErrorCode());
            assertTrue(largeQueryToBeKilled2.getFailureReason().isPresent(), "Query should be killed");
            Throwable failureReason2 = largeQueryToBeKilled2.getFailureReason().get();
            assertTrue(failureReason2 instanceof PrestoException);
            assertEquals(((PrestoException) failureReason2).getErrorCode(), QUERY_HAS_TOO_MANY_STAGES.toErrorCode());
        }
        finally {
            scheduledExecutorService.shutdownNow();
        }
    }
}
