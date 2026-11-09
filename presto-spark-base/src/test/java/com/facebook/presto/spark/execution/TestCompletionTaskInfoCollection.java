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

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryCompletionValidator;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spark.PrestoSparkQueryData;
import com.facebook.presto.spark.PrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.PrestoSparkQueryRunner;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionModule;
import com.facebook.presto.spark.execution.property.NativeExecutionConfigModule;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.RuntimeMetricName.QUERY_COMPLETION_VALIDATION_SKIPPED;
import static com.facebook.presto.common.RuntimeMetricName.TASK_ELAPSED_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskState.FINISHED;
import static com.facebook.presto.execution.TaskStatus.failWith;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestCompletionTaskInfoCollection
{
    @Test
    public void testFirstSuccessfulAttemptAndItsStatisticsAreRetained()
    {
        Map<String, TaskInfo> infos = new HashMap<>();
        TaskInfo first = info(FINISHED, 0);
        first.getStats().getRuntimeStats().addMetricValue(TASK_ELAPSED_TIME_NANOS, NANO, 100);
        TaskInfo later = info(FINISHED, 1);
        later.getStats().getRuntimeStats().addMetricValue(TASK_ELAPSED_TIME_NANOS, NANO, 200);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, first);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, later);
        assertEquals(infos.size(), 1);
        TaskInfo retained = infos.values().iterator().next();
        assertSame(retained, first);
        assertEquals(retained.getStats().getRuntimeStats().getMetric(TASK_ELAPSED_TIME_NANOS).getSum(), 100L);
    }

    @Test
    public void testRecomputationReusingTaskIdRetainsFirstSuccess()
    {
        Map<String, TaskInfo> infos = new HashMap<>();
        TaskInfo first = info(FINISHED, 0);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, first);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, info(FINISHED, 0));
        assertEquals(infos.size(), 1);
        assertSame(infos.values().iterator().next(), first);
    }

    @Test
    public void testSuccessfulRetryReplacesFailedAttempt()
    {
        Map<String, TaskInfo> infos = new HashMap<>();
        TaskInfo success = info(FINISHED, 1);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, info(FAILED, 0));
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, success);
        assertSame(infos.values().iterator().next(), success);
    }

    @Test
    public void testLaterFailureDoesNotReplaceSuccess()
    {
        Map<String, TaskInfo> infos = new HashMap<>();
        TaskInfo success = info(FINISHED, 0);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, success);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, info(FAILED, 1));
        assertSame(infos.values().iterator().next(), success);
    }

    @Test
    public void testHighestFailedAttemptIsRetainedRegardlessOfArrivalOrder()
    {
        Map<String, TaskInfo> infos = new HashMap<>();
        TaskInfo latest = info(FAILED, 2);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, info(FAILED, 0));
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, latest);
        AbstractPrestoSparkQueryExecution.updateTaskInfoMap(infos, info(FAILED, 1));
        assertSame(infos.values().iterator().next(), latest);
    }

    @DataProvider
    public Object[][] completionCollectionCases()
    {
        return new Object[][] {
                {true, false, false},
                {true, true, false},
                {true, false, true},
                {false, false, false}};
    }

    @Test(dataProvider = "completionCollectionCases", timeOut = 180000)
    public void testCompletionStatisticsAreReleasedAfterEvent(boolean validationEnabled, boolean malformedStatistics, boolean failFirstEvent)
    {
        RecordingCompletionValidator validator = new RecordingCompletionValidator(validationEnabled);
        try (PrestoSparkQueryRunner runner = new PrestoSparkQueryRunner(
                "hive",
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableList.of(
                        new NativeExecutionModule(),
                        new NativeExecutionConfigModule(ImmutableMap.of(), ImmutableMap.of("hive", ImmutableMap.of("connector.name", "hive"))),
                        new AbstractModule()
                        {
                            @Override
                            protected void configure()
                            {
                                newSetBinder(binder(), QueryCompletionValidator.class).addBinding().toInstance(validator);
                            }
                        }),
                4)) {
            List<QueryCompletedEvent> completedEvents = new ArrayList<>();
            runner.getEventListenerManager().addEventListenerFactory(new EventListenerFactory()
            {
                @Override
                public String getName()
                {
                    return "completion-collection-test";
                }

                @Override
                public EventListener create(Map<String, String> config)
                {
                    return new EventListener()
                    {
                        @Override
                        public void queryCompleted(QueryCompletedEvent event)
                        {
                            completedEvents.add(event);
                            if (failFirstEvent && completedEvents.size() == 1) {
                                throw new IllegalStateException("Completion event consumer failed");
                            }
                        }
                    };
                }
            });
            runner.getEventListenerManager().loadConfiguredEventListener(ImmutableMap.of("event-listener.name", "completion-collection-test"));
            Session session = Session.builder(runner.getDefaultSession())
                    .setSchema("hive_test")
                    .setSystemProperty("native_execution_enabled", "false")
                    .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "false")
                    .build();
            AbstractPrestoSparkQueryExecution execution = (AbstractPrestoSparkQueryExecution) runner.createPrestoSparkQueryExecution(session, "SELECT 1", ImmutableList.of());
            CountingSerializedTaskInfo malformed = new CountingSerializedTaskInfo();
            if (malformedStatistics) {
                execution.taskInfoCollector.add(malformed);
            }

            List<List<Object>> results = execution.execute();
            assertEquals(results, ImmutableList.of(ImmutableList.of(1)));

            assertTrue(execution.getFinalFragmentedPlan().isPresent());
            assertTrue(execution.taskInfoCollector.isZero());
            assertEquals(completedEvents.size(), 1);
            QueryCompletedEvent event = completedEvents.get(0);
            assertEquals(event.getMetadata().getQueryState(), "FINISHED");
            assertEquals(event.getMetadata().getPayload().isPresent(), !malformedStatistics);
            if (malformedStatistics) {
                assertEquals(malformed.reads.get(), 1);
                assertEquals(validator.calls.get(), 0);
                assertEquals(execution.session.getRuntimeStats().getMetric(QUERY_COMPLETION_VALIDATION_SKIPPED).getSum(), 1L);
                assertTrue(event.getWarnings().stream().anyMatch(warning -> warning.getMessage().contains("final task statistics could not be collected")));
            }
            else {
                assertEquals(event.getStatistics().getOutputRows(), 1L);
                assertEquals(validator.calls.get() > 0, validationEnabled);
            }

            CountingSerializedTaskInfo lateTaskInfo = new CountingSerializedTaskInfo();
            execution.taskInfoCollector.add(lateTaskInfo);
            assertTrue(execution.queryDataJsonCodec.toJsonBytes(new PrestoSparkQueryData(
                    PrestoSparkQueryExecutionFactory.getOutputColumns(execution.planAndMore), results)).length > 0);
            execution.queryCompletedEvent(Optional.empty(), OptionalLong.empty());

            assertEquals(completedEvents.size(), 2);
            assertFalse(completedEvents.get(1).getMetadata().getPayload().isPresent());
            assertEquals(lateTaskInfo.reads.get(), 0);
            assertEquals(execution.taskInfoCollector.value(), ImmutableList.of(lateTaskInfo));
        }
    }

    private static final class RecordingCompletionValidator
            implements QueryCompletionValidator
    {
        private final AtomicInteger calls = new AtomicInteger();
        private final boolean enabled;

        private RecordingCompletionValidator(boolean enabled)
        {
            this.enabled = enabled;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return enabled;
        }

        @Override
        public void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo)
        {
            calls.incrementAndGet();
        }
    }

    private static final class CountingSerializedTaskInfo
            extends SerializedTaskInfo
    {
        private final AtomicInteger reads = new AtomicInteger();

        private CountingSerializedTaskInfo()
        {
            super(new byte[] {1});
        }

        @Override
        public byte[] getBytesAndClear()
        {
            reads.incrementAndGet();
            return super.getBytesAndClear();
        }
    }

    private static TaskInfo info(TaskState state, int attempt)
    {
        TaskInfo initial = TaskInfo.createInitialTask(new TaskId("query", 0, 0, 0, attempt),
                URI.create("http://localhost/task"), ImmutableList.of(), new TaskStats(1L, 2L), "node");
        return initial.withTaskStatus(failWith(initial.getTaskStatus(), state, ImmutableList.of()));
    }
}
