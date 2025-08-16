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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.spark.execution.http.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient;
import com.facebook.presto.sql.planner.PlanFragment;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHttpNativeExecutionTaskInfoFetcher
{
    private static final URI BASE_URI = URI.create("http://localhost");
    private static final TaskId TEST_TASK_ID = TaskId.valueOf("test.0.0.0.0");
    private static final JsonCodec<TaskInfo> TASK_INFO_JSON_CODEC = JsonCodec.jsonCodec(TaskInfo.class);
    private static final JsonCodec<PlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(PlanFragment.class);
    private static final JsonCodec<BatchTaskUpdateRequest> TASK_UPDATE_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(BatchTaskUpdateRequest.class);
    private static final ScheduledExecutorService updateScheduledExecutor = newScheduledThreadPool(4);

    @Test
    public void testNativeExecutionTaskFailsWhenProcessCrashes()
    {
        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestPrestoSparkHttpClient.TestingOkHttpClient(
                        updateScheduledExecutor,
                        new TestPrestoSparkHttpClient.TestingResponseManager(
                                TEST_TASK_ID.toString(),
                                new TestPrestoSparkHttpClient.TestingResponseManager.CrashingTaskInfoResponseManager())),
                TEST_TASK_ID,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                // very low tolerance for error for unit testing
                new Duration(1, TimeUnit.MILLISECONDS),
                updateScheduledExecutor,
                updateScheduledExecutor,
                new Duration(1, TimeUnit.SECONDS));

        Object taskFinishedOrLostSignal = new Object();

        HttpNativeExecutionTaskInfoFetcher taskInfoFetcher = new HttpNativeExecutionTaskInfoFetcher(
                updateScheduledExecutor,
                workerClient,
                new Duration(1, TimeUnit.SECONDS),
                taskFinishedOrLostSignal);

        // set up a listener for the notification
        AtomicBoolean notifyCalled = new AtomicBoolean(false);

        // As there is no easy way to test that notify was called,
        // we use this test object as a way to capture a side effect
        // of notify being called
        Object testSignallingObject = new Object();

        Thread observerThread = new Thread(() -> {
            try {
                synchronized (taskFinishedOrLostSignal) {
                    while (!Thread.interrupted() && taskInfoFetcher.getLastException().get() != null) {
                        taskFinishedOrLostSignal.wait();
                    }
                    notifyCalled.set(true);
                    synchronized (testSignallingObject) {
                        testSignallingObject.notifyAll();
                    }
                }
            }
            catch (InterruptedException ex) {
                fail("Error in test observer thread waiting for notification from info fetcher");
            }
        });
        observerThread.start();

        taskInfoFetcher.doGetTaskInfo();

        //test that notify was called
        try {
            synchronized (testSignallingObject) {
                while (!notifyCalled.get()) {
                    testSignallingObject.wait();
                }
            }
        }
        catch (InterruptedException ex) {
            fail("Exception while waiting for info fetcher to signal process crash", ex);
        }
        observerThread.interrupt();

        assertTrue(notifyCalled.get());
    }
}
