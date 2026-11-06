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
import com.google.common.primitives.Ints;
import org.apache.spark.util.CollectionAccumulator;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static com.facebook.presto.execution.TaskState.ABORTED;
import static com.facebook.presto.execution.TaskState.CANCELED;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskState.FINISHED;
import static com.facebook.presto.execution.TaskState.RUNNING;
import static com.facebook.presto.spark.execution.TaskInfoAggregationMode.INCREMENTAL;
import static com.facebook.presto.spark.execution.TaskInfoAggregationMode.LEGACY;
import static com.facebook.presto.spark.execution.TaskInfoAggregator.isIncrementalAggregationSupported;
import static com.facebook.presto.spark.execution.TaskInfoAggregator.resolveMode;
import static com.facebook.presto.spark.execution.TestTaskInfoDeduplicator.createRandomSequence;
import static com.facebook.presto.spark.execution.TestTaskInfoDeduplicator.selectWithUpdateTaskInfoMap;
import static com.facebook.presto.spark.execution.TestingTaskInfos.SPARK_VERSION;
import static com.facebook.presto.spark.execution.TestingTaskInfos.createTaskInfo;
import static com.facebook.presto.spark.execution.TestingTaskInfos.createTaskInfoCollector;
import static com.facebook.presto.spark.execution.TestingTaskInfos.createTaskStats;
import static com.facebook.presto.spark.util.PrestoSparkUtils.serializeZstdCompressed;
import static com.google.common.collect.ImmutableList.sortedCopyOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestTaskInfoAggregator
{
    private static final Duration DRAIN_INTERVAL = new Duration(1, MILLISECONDS);
    private static final Duration SEAL_TIMEOUT = new Duration(1, MINUTES);
    private static final DataSize MAX_BACKLOG_SIZE = new DataSize(1, GIGABYTE);
    private static final AtomicInteger queryIdSequence = new AtomicInteger();

    @Test
    public void testSparkVersionGuard()
    {
        for (String version : ImmutableList.of("2.0.2", "2.4.4", "3.0.0", "3.0.3", "3.1.2", "3", "", "unknown", "3.x.1", "x.4.1")) {
            assertFalse(isIncrementalAggregationSupported(version), version);
            assertEquals(resolveMode(INCREMENTAL, version), LEGACY, version);
        }
        for (String version : ImmutableList.of("3.2.0", "3.4.1", "3.4.15341", "4.0.0")) {
            assertTrue(isIncrementalAggregationSupported(version), version);
            assertEquals(resolveMode(INCREMENTAL, version), INCREMENTAL, version);
        }
        assertEquals(resolveMode(LEGACY, "3.4.1"), LEGACY);
        assertEquals(resolveMode(LEGACY, "2.0.2"), LEGACY);
    }

    @Test
    public void testAccumulatorLockingContract()
            throws Exception
    {
        CollectionAccumulator<SerializedTaskInfo> collector = new CollectionAccumulator<>();
        CollectionAccumulator<SerializedTaskInfo> update = new CollectionAccumulator<>();
        update.add(new SerializedTaskInfo(new byte[1]));
        CountDownLatch merged = new CountDownLatch(1);
        Thread merger = new Thread(() -> {
            collector.merge(update);
            merged.countDown();
        });
        boolean mergeBlocked;
        synchronized (collector) {
            merger.start();
            mergeBlocked = !merged.await(1, SECONDS);
        }
        assertTrue(merged.await(1, MINUTES));
        merger.join();
        assertEquals(collector.value().size(), 1);
        // draining is atomic only if a merge blocks while the drainer holds the accumulator monitor
        assertEquals(mergeBlocked, isIncrementalAggregationSupported(SPARK_VERSION), "Spark " + SPARK_VERSION);
    }

    @Test
    public void testFoldsSelectedAttempts()
    {
        QueryId queryId = nextQueryId();
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        addAll(collector, codec, ImmutableList.of(
                createTaskInfo(1, 0, 0, FAILED, "1.0.0"),
                createTaskInfo(1, 0, 1, FINISHED, "1.0.1"),
                createTaskInfo(1, 0, 2, FINISHED, "1.0.2"),
                createTaskInfo(1, 1, 0, RUNNING, "1.1.0"),
                createTaskInfo(1, 1, 1, FAILED, "1.1.1"),
                createTaskInfo(1, 1, 1, ABORTED, "1.1.1-duplicate"),
                createTaskInfo(1, 1, 0, FAILED, "1.1.0-stage-reattempt"),
                createTaskInfo(2, 0, 0, FINISHED, "2.0.0"),
                createTaskInfo(1, 0, 0, CANCELED, "1.0.0-late")));
        TestingTaskInfoSink sink = new TestingTaskInfoSink();

        TaskInfoAggregationResult<List<TaskInfo>> result = startAndSeal(createAggregator(queryId, collector, codec::decode, sink));

        assertTrue(result.isComplete(), result.toString());
        assertFalse(result.isDegraded());
        assertFalse(result.isTimedOut());
        // finished attempts are folded as they arrive, the pending attempts at seal in partition order
        assertEquals(getMarkers(result.getSinkResult().get()), ImmutableList.of("1.0.1", "2.0.0", "1.1.1"));
        assertEquals(result.getReceivedCount(), 9);
        assertEquals(result.getFoldedCount(), 3);
        assertEquals(result.getDuplicateFinishedCount(), 1);
        assertEquals(result.getSupersededCount(), 5);
        assertEquals(result.getPendingFoldedAtSealCount(), 1);
        assertAccounted(result);

        Thread thread = sink.getThread();
        assertEquals(thread.getName(), "presto-spark-task-info-aggregator-" + queryId);
        assertTrue(thread.isDaemon());
        assertSame(thread.getContextClassLoader(), TaskInfoAggregator.class.getClassLoader());
        assertNoAggregatorThread(queryId);
    }

    @Test
    public void testMatchesUpdateTaskInfoMap()
    {
        Logger legacyLogger = Logger.getLogger(AbstractPrestoSparkQueryExecution.class.getName());
        Level legacyLevel = legacyLogger.getLevel();
        // updateTaskInfoMap warns about every duplicate attempt
        legacyLogger.setLevel(Level.OFF);
        try {
            Random random = new Random(20261008);
            for (int sequence = 0; sequence < 20; sequence++) {
                List<TaskInfo> taskInfos = createRandomSequence(random);
                CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
                TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
                TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(nextQueryId(), collector, codec::decode, new TestingTaskInfoSink());
                TaskInfoAggregationResult<List<TaskInfo>> result;
                // either one drain, or batches split wherever the drains happen to fall
                if (sequence % 2 == 0) {
                    addAll(collector, codec, taskInfos);
                    aggregator.start();
                }
                else {
                    aggregator.start();
                    addAll(collector, codec, taskInfos);
                }
                try {
                    result = aggregator.seal();
                }
                finally {
                    aggregator.close();
                }

                assertTrue(result.isComplete(), result.toString());
                assertAccounted(result);
                Map<String, String> selected = result.getSinkResult().get().stream()
                        .collect(toImmutableMap(TestTaskInfoDeduplicator::getTaskKey, TaskInfo::getNodeId));
                assertEquals(selected, selectWithUpdateTaskInfoMap(taskInfos), "sequence " + sequence);
            }
        }
        finally {
            legacyLogger.setLevel(legacyLevel);
        }
    }

    @Test
    public void testDecodesWithTaskInfoCodec()
    {
        JsonCodec<TaskInfo> codec = jsonCodec(TaskInfo.class);
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TaskInfo finished = createTaskInfo(3, 7, 1, FINISHED, "finished", System.currentTimeMillis() - 10_000, createTaskStats(1, 2, 3, 4));
        TaskInfo failed = createTaskInfo(3, 8, 0, FAILED, "failed");
        collector.add(new SerializedTaskInfo(serializeZstdCompressed(codec, finished)));
        collector.add(new SerializedTaskInfo(serializeZstdCompressed(codec, failed)));
        TestingTaskInfoSink sink = new TestingTaskInfoSink();

        TaskInfoAggregationResult<List<TaskInfo>> result = startAndSeal(new TaskInfoAggregator<>(nextQueryId(), collector, codec, sink, DRAIN_INTERVAL, SEAL_TIMEOUT, MAX_BACKLOG_SIZE));

        assertTrue(result.isComplete(), result.toString());
        List<TaskInfo> folded = result.getSinkResult().get();
        assertEquals(folded.stream().map(TaskInfo::getTaskId).collect(toImmutableList()), ImmutableList.of(finished.getTaskId(), failed.getTaskId()));
        assertEquals(folded.get(0).getTaskStatus().getState(), FINISHED);
        assertEquals(folded.get(0).getStats().getPeakNodeTotalMemoryInBytes(), 4);
        assertEquals(folded.get(1).getTaskStatus().getState(), FAILED);
        assertEquals(result.getPendingFoldedAtSealCount(), 1);
        assertTrue(result.getDecodeTimeNanos() > 0);
        assertTrue(result.getMaxFoldLagMillis() >= 10_000, result.toString());
        assertTrue(result.getAverageFoldLagMillis() >= 10_000, result.toString());
        assertTrue(result.getMaxBacklogBytes() > 0);
    }

    @Test
    public void testConcurrentMergesAreDrainedExactlyOnce()
            throws Exception
    {
        int mergerCount = 4;
        int partitionsPerMerger = 500;
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        Queue<String> decodedMarkers = new ConcurrentLinkedQueue<>();
        TestingTaskInfoSink sink = new TestingTaskInfoSink();
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(nextQueryId(), collector, compressedTaskInfo -> {
            TaskInfo taskInfo = codec.decode(compressedTaskInfo);
            decodedMarkers.add(taskInfo.getNodeId());
            return taskInfo;
        }, sink);

        List<String> expectedMarkers = new ArrayList<>();
        List<String> expectedFoldedMarkers = new ArrayList<>();
        TaskInfoAggregationResult<List<TaskInfo>> result;
        aggregator.start();
        try {
            ExecutorService executor = newFixedThreadPool(mergerCount);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int merger = 0; merger < mergerCount; merger++) {
                    int fragmentId = merger;
                    for (int partition = 0; partition < partitionsPerMerger; partition++) {
                        expectedMarkers.add(fragmentId + "." + partition + ".0");
                        expectedMarkers.add(fragmentId + "." + partition + ".1");
                        expectedFoldedMarkers.add(fragmentId + "." + partition + ".1");
                    }
                    futures.add(executor.submit(() -> {
                        for (int partition = 0; partition < partitionsPerMerger; partition++) {
                            // like Spark, merge the failed attempt before the retry that finishes
                            merge(collector, codec.encode(createTaskInfo(fragmentId, partition, 0, FAILED, fragmentId + "." + partition + ".0")));
                            merge(collector, codec.encode(createTaskInfo(fragmentId, partition, 1, FINISHED, fragmentId + "." + partition + ".1")));
                            if (partition % 10 == 0) {
                                Thread.sleep(1);
                            }
                        }
                        return null;
                    }));
                }
                for (Future<?> future : futures) {
                    future.get(1, MINUTES);
                }
            }
            finally {
                executor.shutdownNow();
            }
            result = aggregator.seal();
        }
        finally {
            aggregator.close();
        }

        int partitionCount = mergerCount * partitionsPerMerger;
        assertTrue(result.isComplete(), result.toString());
        assertEquals(sortedCopyOf(decodedMarkers), sortedCopyOf(expectedMarkers));
        assertEquals(sortedCopyOf(getMarkers(result.getSinkResult().get())), sortedCopyOf(expectedFoldedMarkers));
        assertEquals(result.getReceivedCount(), 2 * partitionCount);
        assertEquals(result.getFoldedCount(), partitionCount);
        assertEquals(result.getSupersededCount(), partitionCount);
        assertEquals(result.getDuplicateFinishedCount(), 0);
        assertEquals(result.getPendingFoldedAtSealCount(), 0);
        assertTrue(collector.value().isEmpty());
    }

    @Test
    public void testFoldsDriverTaskInfosAtSeal()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        collector.add(codec.encode(createTaskInfo(1, 0, 0, FINISHED, "executor"), 100));
        SerializedTaskInfo driverTaskInfo = codec.encode(createTaskInfo(0, 0, 0, FINISHED, "driver"), 1000);
        SerializedTaskInfo failedDriverTaskInfo = codec.encode(createTaskInfo(2, 0, 0, FAILED, "failed-driver"), 1000);
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(nextQueryId(), collector, codec::decode, new TestingTaskInfoSink(), SEAL_TIMEOUT, new DataSize(100, BYTE));

        TaskInfoAggregationResult<List<TaskInfo>> result;
        aggregator.start();
        try {
            result = aggregator.seal(ImmutableList.of(driverTaskInfo, failedDriverTaskInfo));
        }
        finally {
            aggregator.close();
        }

        // the driver task infos exceed the backlog limit, but are never dropped
        assertTrue(result.isComplete(), result.toString());
        assertEquals(getMarkers(result.getSinkResult().get()), ImmutableList.of("executor", "driver", "failed-driver"));
        assertEquals(result.getReceivedCount(), 3);
        assertEquals(result.getPendingFoldedAtSealCount(), 1);
        assertAccounted(result);
        assertEquals(driverTaskInfo.getBytes().length, 1000);
    }

    @Test
    public void testDecodeFailuresAreContained()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        SerializedTaskInfo cleared = codec.encode(createTaskInfo(1, 3, 0, FINISHED, "cleared"));
        cleared.getBytesAndClear();
        collector.add(codec.encode(createTaskInfo(1, 0, 0, FINISHED, "good-0")));
        collector.add(codec.encode(createTaskInfo(1, 1, 0, FINISHED, "corrupt")));
        collector.add(cleared);
        collector.add(codec.encode(createTaskInfo(1, 2, 0, FINISHED, "good-2")));
        Function<byte[], TaskInfo> decoder = compressedTaskInfo -> {
            TaskInfo taskInfo = codec.decode(compressedTaskInfo);
            if (taskInfo.getNodeId().equals("corrupt")) {
                throw new IllegalArgumentException("corrupt task info");
            }
            return taskInfo;
        };

        TaskInfoAggregationResult<List<TaskInfo>> result = startAndSeal(createAggregator(nextQueryId(), collector, decoder, new TestingTaskInfoSink()));

        assertTrue(result.isPartial());
        assertFalse(result.isComplete());
        assertFalse(result.isDegraded());
        assertEquals(getMarkers(result.getSinkResult().get()), ImmutableList.of("good-0", "good-2"));
        assertEquals(result.getFoldFailureCount(), 2);
        assertAccounted(result);
    }

    @Test
    public void testSinkFailuresAreContained()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        addAll(collector, codec, ImmutableList.of(
                createTaskInfo(1, 0, 0, FINISHED, "good"),
                createTaskInfo(1, 1, 0, FINISHED, "rejected"),
                createTaskInfo(1, 1, 1, FINISHED, "duplicate")));
        TestingTaskInfoSink sink = new TestingTaskInfoSink()
        {
            @Override
            public void addTask(int fragmentId, TaskInfo taskInfo)
            {
                if (taskInfo.getNodeId().equals("rejected")) {
                    throw new IllegalStateException("injected sink failure");
                }
                super.addTask(fragmentId, taskInfo);
            }
        };

        TaskInfoAggregationResult<List<TaskInfo>> result = startAndSeal(createAggregator(nextQueryId(), collector, codec::decode, sink));

        assertTrue(result.isPartial());
        assertFalse(result.isDegraded());
        assertEquals(getMarkers(result.getSinkResult().get()), ImmutableList.of("good"));
        assertEquals(result.getFoldFailureCount(), 1);
        // the failed attempt stays selected, as it would have been without incremental aggregation
        assertEquals(result.getDuplicateFinishedCount(), 1);
        assertAccounted(result);
    }

    @Test
    public void testSinkSealFailureDegrades()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        addAll(collector, codec, ImmutableList.of(createTaskInfo(1, 0, 0, FINISHED, "0"), createTaskInfo(1, 1, 0, FINISHED, "1")));
        TestingTaskInfoSink sink = new TestingTaskInfoSink()
        {
            @Override
            public List<TaskInfo> seal()
            {
                throw new IllegalStateException("injected sink failure");
            }
        };

        TaskInfoAggregationResult<List<TaskInfo>> result = startAndSeal(createAggregator(nextQueryId(), collector, codec::decode, sink));

        assertTrue(result.isDegraded());
        assertFalse(result.isComplete());
        assertFalse(result.getSinkResult().isPresent());
        assertEquals(result.getFoldedCount(), 2);
    }

    @Test
    public void testErrorDegrades()
    {
        QueryId queryId = nextQueryId();
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        addAll(collector, codec, ImmutableList.of(createTaskInfo(1, 0, 0, FINISHED, "0"), createTaskInfo(1, 1, 0, FINISHED, "error")));
        Function<byte[], TaskInfo> decoder = compressedTaskInfo -> {
            TaskInfo taskInfo = codec.decode(compressedTaskInfo);
            if (taskInfo.getNodeId().equals("error")) {
                throw new AssertionError("injected error");
            }
            return taskInfo;
        };

        TaskInfoAggregationResult<List<TaskInfo>> result = startAndSeal(createAggregator(queryId, collector, decoder, new TestingTaskInfoSink()));

        assertTrue(result.isDegraded());
        assertFalse(result.getSinkResult().isPresent());
        assertEquals(result.getReceivedCount(), 2);
        assertEquals(result.getFoldedCount(), 1);
        assertNoAggregatorThread(queryId);
    }

    @Test
    public void testSealTimeout()
    {
        QueryId queryId = nextQueryId();
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        collector.add(codec.encode(createTaskInfo(1, 0, 0, FINISHED, "0")));
        CountDownLatch release = new CountDownLatch(1);
        TestingTaskInfoSink sink = new TestingTaskInfoSink()
        {
            @Override
            public void addTask(int fragmentId, TaskInfo taskInfo)
            {
                awaitUninterruptibly(release);
                super.addTask(fragmentId, taskInfo);
            }
        };
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(queryId, collector, codec::decode, sink, new Duration(100, MILLISECONDS), MAX_BACKLOG_SIZE);
        aggregator.start();

        TaskInfoAggregationResult<List<TaskInfo>> result = aggregator.seal();
        assertTrue(result.isTimedOut());
        assertFalse(result.isComplete());
        assertFalse(result.getSinkResult().isPresent());
        assertTrue(result.getSealWaitNanos() >= MILLISECONDS.toNanos(100), result.toString());
        assertSame(aggregator.seal(), result);

        release.countDown();
        aggregator.close();
        assertNoAggregatorThread(queryId);
    }

    @Test
    public void testSealAndCloseAreIdempotent()
    {
        QueryId queryId = nextQueryId();
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        collector.add(codec.encode(createTaskInfo(1, 0, 0, FINISHED, "0")));
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(queryId, collector, codec::decode, new TestingTaskInfoSink());
        aggregator.start();
        aggregator.start();

        TaskInfoAggregationResult<List<TaskInfo>> result = aggregator.seal();
        assertTrue(result.isComplete(), result.toString());
        assertSame(aggregator.seal(), result);
        aggregator.close();
        aggregator.close();
        assertSame(aggregator.seal(), result);
        aggregator.start();
        assertNoAggregatorThread(queryId);
    }

    @Test
    public void testCloseBeforeSeal()
    {
        QueryId queryId = nextQueryId();
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        collector.add(codec.encode(createTaskInfo(1, 0, 0, FINISHED, "0")));
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(queryId, collector, codec::decode, new TestingTaskInfoSink());
        aggregator.start();
        aggregator.close();

        assertNoAggregatorThread(queryId);
        assertTrue(collector.value().isEmpty());
        TaskInfoAggregationResult<List<TaskInfo>> result = aggregator.seal();
        assertTrue(result.isDegraded());
        assertFalse(result.getSinkResult().isPresent());
    }

    @Test
    public void testSealWithoutStart()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        collector.add(codec.encode(createTaskInfo(1, 0, 0, FINISHED, "0")));
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(nextQueryId(), collector, codec::decode, new TestingTaskInfoSink());

        TaskInfoAggregationResult<List<TaskInfo>> result = aggregator.seal();
        assertTrue(result.isDegraded());
        assertFalse(result.getSinkResult().isPresent());
        aggregator.close();
        assertEquals(collector.value().size(), 1);
    }

    @Test
    public void testTaskInfosAfterSealAreDropped()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        collector.add(codec.encode(createTaskInfo(1, 0, 0, FINISHED, "0")));
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(nextQueryId(), collector, codec::decode, new TestingTaskInfoSink());
        aggregator.start();

        TaskInfoAggregationResult<List<TaskInfo>> result = aggregator.seal();
        collector.add(codec.encode(createTaskInfo(1, 1, 0, FINISHED, "1")));
        collector.add(codec.encode(createTaskInfo(1, 2, 0, FINISHED, "2")));
        aggregator.close();

        assertTrue(collector.value().isEmpty());
        assertEquals(result.getReceivedCount(), 1);
        assertEquals(getMarkers(result.getSinkResult().get()), ImmutableList.of("0"));
    }

    @Test
    public void testBacklogLimitDropsOldestTaskInfos()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        for (int partition = 0; partition < 25; partition++) {
            collector.add(codec.encode(createTaskInfo(1, partition, 0, FINISHED, String.valueOf(partition)), 100));
        }
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(nextQueryId(), collector, codec::decode, new TestingTaskInfoSink(), SEAL_TIMEOUT, new DataSize(1000, BYTE));

        TaskInfoAggregationResult<List<TaskInfo>> result = startAndSeal(aggregator);

        assertTrue(result.isPartial());
        assertFalse(result.isComplete());
        assertEquals(getMarkers(result.getSinkResult().get()), ImmutableList.of("15", "16", "17", "18", "19", "20", "21", "22", "23", "24"));
        assertEquals(result.getDroppedCount(), 15);
        assertEquals(result.getDroppedBytes(), 1500);
        assertEquals(result.getMaxBacklogBytes(), 1000);
        assertAccounted(result);
    }

    @Test
    public void testPendingAttemptsCountAgainstBacklogLimit()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        CountDownLatch decoded = new CountDownLatch(3);
        Function<byte[], TaskInfo> decoder = compressedTaskInfo -> {
            decoded.countDown();
            return codec.decode(compressedTaskInfo);
        };
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(nextQueryId(), collector, decoder, new TestingTaskInfoSink(), SEAL_TIMEOUT, new DataSize(300, BYTE));
        for (int partition = 0; partition < 3; partition++) {
            collector.add(codec.encode(createTaskInfo(1, partition, 0, FAILED, "failed-" + partition), 100));
        }

        TaskInfoAggregationResult<List<TaskInfo>> result;
        aggregator.start();
        try {
            awaitUninterruptibly(decoded);
            collector.add(codec.encode(createTaskInfo(1, 0, 1, FINISHED, "finished-0"), 100));
            result = aggregator.seal();
        }
        finally {
            aggregator.close();
        }

        assertTrue(result.isPartial());
        assertEquals(getMarkers(result.getSinkResult().get()), ImmutableList.of("failed-0", "failed-1", "failed-2"));
        assertEquals(result.getDroppedCount(), 1);
        assertEquals(result.getPendingFoldedAtSealCount(), 3);
        assertEquals(result.getMaxBacklogBytes(), 300);
        assertAccounted(result);
    }

    @Test
    public void testPendingAttemptsCanExceedBacklogLimit()
    {
        CollectionAccumulator<SerializedTaskInfo> collector = createTaskInfoCollector();
        TestingTaskInfoCodec codec = new TestingTaskInfoCodec();
        List<SerializedTaskInfo> driverTaskInfos = ImmutableList.of(
                codec.encode(createTaskInfo(0, 0, 0, FAILED, "failed-0"), 200),
                codec.encode(createTaskInfo(0, 1, 0, FAILED, "failed-1"), 200));
        TaskInfoAggregator<List<TaskInfo>> aggregator = createAggregator(nextQueryId(), collector, codec::decode, new TestingTaskInfoSink(), SEAL_TIMEOUT, new DataSize(300, BYTE));

        TaskInfoAggregationResult<List<TaskInfo>> result;
        aggregator.start();
        try {
            result = aggregator.seal(driverTaskInfos);
        }
        finally {
            aggregator.close();
        }

        // pending attempts are never dropped, so the retained size exceeds the limit
        assertTrue(result.isComplete(), result.toString());
        assertEquals(result.getMaxBacklogBytes(), 400);
        assertEquals(result.getPendingFoldedAtSealCount(), 2);
        assertEquals(result.getDroppedCount(), 0);
    }

    private static QueryId nextQueryId()
    {
        return new QueryId("test_task_info_aggregator_" + queryIdSequence.incrementAndGet());
    }

    private static TaskInfoAggregator<List<TaskInfo>> createAggregator(
            QueryId queryId,
            CollectionAccumulator<SerializedTaskInfo> collector,
            Function<byte[], TaskInfo> decoder,
            TaskInfoSink<List<TaskInfo>> sink)
    {
        return createAggregator(queryId, collector, decoder, sink, SEAL_TIMEOUT, MAX_BACKLOG_SIZE);
    }

    private static TaskInfoAggregator<List<TaskInfo>> createAggregator(
            QueryId queryId,
            CollectionAccumulator<SerializedTaskInfo> collector,
            Function<byte[], TaskInfo> decoder,
            TaskInfoSink<List<TaskInfo>> sink,
            Duration sealTimeout,
            DataSize maxBacklogSize)
    {
        return new TaskInfoAggregator<>(queryId, collector, decoder, sink, DRAIN_INTERVAL, sealTimeout, maxBacklogSize);
    }

    private static <R> TaskInfoAggregationResult<R> startAndSeal(TaskInfoAggregator<R> aggregator)
    {
        aggregator.start();
        try {
            return aggregator.seal();
        }
        finally {
            aggregator.close();
        }
    }

    private static void addAll(CollectionAccumulator<SerializedTaskInfo> collector, TestingTaskInfoCodec codec, List<TaskInfo> taskInfos)
    {
        for (TaskInfo taskInfo : taskInfos) {
            collector.add(codec.encode(taskInfo));
        }
    }

    private static void merge(CollectionAccumulator<SerializedTaskInfo> collector, SerializedTaskInfo taskInfo)
    {
        CollectionAccumulator<SerializedTaskInfo> update = new CollectionAccumulator<>();
        update.add(taskInfo);
        collector.merge(update);
    }

    private static List<String> getMarkers(List<TaskInfo> taskInfos)
    {
        return taskInfos.stream()
                .map(TaskInfo::getNodeId)
                .collect(toImmutableList());
    }

    private static void assertAccounted(TaskInfoAggregationResult<?> result)
    {
        long accountedCount = result.getFoldedCount() + result.getDuplicateFinishedCount() + result.getSupersededCount() + result.getFoldFailureCount() + result.getDroppedCount();
        assertEquals(accountedCount, result.getReceivedCount(), result.toString());
    }

    private static void assertNoAggregatorThread(QueryId queryId)
    {
        String threadName = "presto-spark-task-info-aggregator-" + queryId;
        assertTrue(Thread.getAllStackTraces().keySet().stream().noneMatch(thread -> thread.getName().equals(threadName)), threadName + " is still running");
    }

    /**
     * Encodes a task info as its index in a registry instead of compressing it.
     */
    private static class TestingTaskInfoCodec
    {
        private final Map<Integer, TaskInfo> taskInfos = new ConcurrentHashMap<>();
        private final AtomicInteger nextId = new AtomicInteger();

        public SerializedTaskInfo encode(TaskInfo taskInfo)
        {
            return encode(taskInfo, Integer.BYTES);
        }

        public SerializedTaskInfo encode(TaskInfo taskInfo, int size)
        {
            int id = nextId.getAndIncrement();
            taskInfos.put(id, taskInfo);
            byte[] bytes = new byte[max(size, Integer.BYTES)];
            System.arraycopy(Ints.toByteArray(id), 0, bytes, 0, Integer.BYTES);
            return new SerializedTaskInfo(bytes);
        }

        public TaskInfo decode(byte[] bytes)
        {
            return requireNonNull(taskInfos.get(Ints.fromBytes(bytes[0], bytes[1], bytes[2], bytes[3])), "unknown task info");
        }
    }
}
