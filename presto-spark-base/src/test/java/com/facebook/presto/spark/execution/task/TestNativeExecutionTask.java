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
package com.facebook.presto.spark.execution.task;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spark.PrestoSparkSessionProperties;
import com.facebook.presto.spark.execution.http.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient.TestingOkHttpClient;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient.TestingResponseManager;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNativeExecutionTask
{
    private static final URI BASE_URI = URI.create("http://localhost:8080");
    private static final JsonCodec<TaskInfo> TASK_INFO_JSON_CODEC = JsonCodec.jsonCodec(TaskInfo.class);
    private static final JsonCodec<PlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(PlanFragment.class);
    private static final JsonCodec<BatchTaskUpdateRequest> TASK_UPDATE_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(BatchTaskUpdateRequest.class);
    // The task update request reads Presto-on-Spark session properties, which the default testing session does not register
    private static final SessionPropertyManager SESSION_PROPERTY_MANAGER = createTestingSessionPropertyManager(
            ImmutableList.<PropertyMetadata<?>>builder()
                    .addAll(new SystemSessionProperties().getSessionProperties())
                    .addAll(new PrestoSparkSessionProperties().getSessionProperties())
                    .build());

    private ScheduledExecutorService scheduledExecutorService;

    @BeforeClass
    public void setUp()
    {
        scheduledExecutorService = newScheduledThreadPool(4);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutorService.shutdownNow();
        scheduledExecutorService = null;
    }

    @Test
    public void testReleaseSourcesBeforeStartFails()
    {
        List<TaskSource> sources = sources();
        NativeExecutionTask task = createTask(new TaskId("testid", 0, 0, 0, 0), sources);

        assertThatThrownBy(task::releaseSources)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before start()");
        assertEquals(task.getSources(), sources);
    }

    @Test
    public void testReleaseSourcesAfterStart()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        List<TaskSource> sources = sources();
        NativeExecutionTask task = createTask(taskId, sources);
        try {
            task.start();
            assertEquals(task.getSources(), sources);

            task.releaseSources();
            assertNull(task.getSources());
        }
        finally {
            task.stop(false);
        }
    }

    private NativeExecutionTask createTask(TaskId taskId, List<TaskSource> sources)
    {
        NativeExecutionTaskFactory taskFactory = new NativeExecutionTaskFactory(
                new TestingOkHttpClient(scheduledExecutorService, new TestingResponseManager(taskId.toString())),
                scheduledExecutorService,
                scheduledExecutorService,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new TaskManagerConfig(),
                new QueryManagerConfig());
        Session session = testSessionBuilder(SESSION_PROPERTY_MANAGER).build();
        return taskFactory.createNativeExecutionTask(
                session,
                BASE_URI,
                taskId,
                createPlanFragment(),
                sources,
                new TableWriteInfo(Optional.empty(), Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                false);
    }

    private static List<TaskSource> sources()
    {
        return ImmutableList.of(new TaskSource(new PlanNodeId("tableScan"), ImmutableSet.of(), true));
    }
}
