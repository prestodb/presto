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
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.CollectionAccumulator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Option;

import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskState.FINISHED;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.acquireSharedSparkContext;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.releaseSharedSparkContext;
import static com.facebook.presto.spark.execution.TestingTaskInfos.createTaskInfo;
import static com.facebook.presto.spark.execution.TestingTaskInfos.createTaskInfoCollector;
import static com.facebook.presto.spark.util.PrestoSparkUtils.deserializeZstdCompressed;
import static com.facebook.presto.spark.util.PrestoSparkUtils.serializeZstdCompressed;
import static com.google.common.collect.ImmutableList.sortedCopyOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Drains an accumulator while a Spark job merges into it on the DAG scheduler thread. On Spark 2 the accumulator
 * emulates the locking of Spark 3, see {@link TestingTaskInfos#createTaskInfoCollector}.
 */
@Test(singleThreaded = true)
public class TestTaskInfoAggregatorWithSparkContext
{
    private static final int PARTITION_COUNT = 200;
    private static final int FRAGMENT_ID = 1;
    private static final JsonCodec<TaskInfo> TASK_INFO_CODEC = jsonCodec(TaskInfo.class);

    private SparkContext sparkContext;

    @BeforeClass
    public void setUp()
    {
        // only one SparkContext can run in a JVM, and the query runners of the tests running in parallel use it too
        sparkContext = acquireSharedSparkContext();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (sparkContext != null) {
            releaseSharedSparkContext(sparkContext);
            sparkContext = null;
        }
    }

    @Test
    public void testSparkMergesAreDrainedExactlyOnce()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        collector.register(sparkContext, Option.empty(), true);
        Queue<String> decodedMarkers = new ConcurrentLinkedQueue<>();
        AtomicLong firstDecodeNanos = new AtomicLong(Long.MAX_VALUE);
        TestingTaskInfoSink sink = new TestingTaskInfoSink();
        TaskInfoAggregator<List<TaskInfo>> aggregator = new TaskInfoAggregator<>(
                new QueryId("test_spark_merges"),
                collector,
                compressedTaskInfo -> {
                    firstDecodeNanos.compareAndSet(Long.MAX_VALUE, System.nanoTime());
                    TaskInfo taskInfo = deserializeZstdCompressed(TASK_INFO_CODEC, compressedTaskInfo);
                    decodedMarkers.add(taskInfo.getNodeId());
                    return taskInfo;
                },
                sink,
                new Duration(1, MILLISECONDS),
                new Duration(1, MINUTES),
                new DataSize(1, GIGABYTE));

        long jobEndNanos;
        TaskInfoAggregationResult<List<TaskInfo>> result;
        aggregator.start();
        try {
            List<Integer> partitions = IntStream.range(0, PARTITION_COUNT).boxed().collect(toImmutableList());
            new JavaSparkContext(sparkContext)
                    .parallelize(partitions, PARTITION_COUNT)
                    .foreach(new AddTaskInfos(collector));
            jobEndNanos = System.nanoTime();
            result = aggregator.seal();
        }
        finally {
            aggregator.close();
        }

        ImmutableList.Builder<String> expectedMarkers = ImmutableList.builder();
        ImmutableList.Builder<String> expectedFoldedMarkers = ImmutableList.builder();
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            for (int attempt = 0; attempt <= getFailedAttemptCount(partition); attempt++) {
                expectedMarkers.add(getMarker(partition, attempt));
            }
            expectedFoldedMarkers.add(getMarker(partition, getFailedAttemptCount(partition)));
        }
        List<String> mergedMarkers = sortedCopyOf(expectedMarkers.build());

        assertTrue(result.isComplete(), result.toString());
        assertTrue(firstDecodeNanos.get() < jobEndNanos, "task infos were not drained while the job was running");
        assertEquals(sortedCopyOf(decodedMarkers), mergedMarkers);
        assertEquals(result.getReceivedCount(), mergedMarkers.size());
        assertEquals(result.getFoldedCount(), PARTITION_COUNT);
        assertEquals(result.getSupersededCount(), mergedMarkers.size() - PARTITION_COUNT);
        assertEquals(result.getDuplicateFinishedCount(), 0);
        assertEquals(result.getPendingFoldedAtSealCount(), 0);
        List<String> foldedMarkers = result.getSinkResult().get().stream()
                .sorted(Comparator.comparingInt(taskInfo -> taskInfo.getTaskId().getId()))
                .map(TaskInfo::getNodeId)
                .collect(toImmutableList());
        assertEquals(foldedMarkers, expectedFoldedMarkers.build());
    }

    private static int getFailedAttemptCount(int partition)
    {
        if (partition % 11 == 0) {
            return 2;
        }
        return partition % 7 == 0 ? 1 : 0;
    }

    private static String getMarker(int partition, int attempt)
    {
        return partition + "." + attempt;
    }

    /**
     * Adds the task infos of the failed attempts of a task, followed by the one of the attempt that finished.
     */
    private static class AddTaskInfos
            implements VoidFunction<Integer>
    {
        private final CollectionAccumulator<SerializedTaskInfo> collector;

        public AddTaskInfos(CollectionAccumulator<SerializedTaskInfo> collector)
        {
            this.collector = collector;
        }

        @Override
        public void call(Integer ignored)
                throws Exception
        {
            int partition = TaskContext.get().partitionId();
            int failedAttemptCount = getFailedAttemptCount(partition);
            for (int attempt = 0; attempt <= failedAttemptCount; attempt++) {
                TaskInfo taskInfo = createTaskInfo(FRAGMENT_ID, partition, attempt, attempt < failedAttemptCount ? FAILED : FINISHED, getMarker(partition, attempt));
                collector.add(new SerializedTaskInfo(serializeZstdCompressed(TASK_INFO_CODEC, taskInfo)));
            }
            // keep the job running across several drains
            Thread.sleep(5);
        }
    }
}
