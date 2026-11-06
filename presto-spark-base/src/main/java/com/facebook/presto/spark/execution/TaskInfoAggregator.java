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

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.CollectionAccumulator;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.facebook.presto.spark.execution.TaskInfoAggregationMode.INCREMENTAL;
import static com.facebook.presto.spark.execution.TaskInfoAggregationMode.LEGACY;
import static com.facebook.presto.spark.execution.TaskInfoDeduplicator.Decision.FOLD;
import static com.facebook.presto.spark.execution.TaskInfoDeduplicator.getFragmentId;
import static com.facebook.presto.spark.util.PrestoSparkUtils.deserializeZstdCompressed;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Aggregates the task infos of a query on a background thread while the query runs. The thread periodically drains
 * the task info accumulator, selects one attempt per task with {@link TaskInfoDeduplicator} and folds it into a
 * {@link TaskInfoSink}, so decoded task infos are never retained.
 * <p>
 * Call {@link #start} when the query starts executing, {@link #seal} when it completes, successfully or not, and
 * {@link #close} in a finally block. Task infos that arrive after the seal are dropped. A failure of the aggregation
 * never propagates: it is counted and reported by the {@link TaskInfoAggregationResult}, and none of these methods
 * throw.
 */
public final class TaskInfoAggregator<R>
        implements Closeable
{
    private static final Logger log = Logger.get(TaskInfoAggregator.class);
    private static final int MAX_LOGGED_FAILURES = 3;
    private static final long CLOSE_TIMEOUT_MILLIS = 10_000;
    private static final AtomicBoolean unsupportedSparkVersionLogged = new AtomicBoolean();

    private final QueryId queryId;
    private final CollectionAccumulator<SerializedTaskInfo> taskInfoCollector;
    private final Function<byte[], TaskInfo> decoder;
    private final TaskInfoSink<R> sink;
    private final long drainIntervalNanos;
    private final long sealTimeoutMillis;
    private final long backlogLimitBytes;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch wakeUp = new CountDownLatch(1);
    private final SettableFuture<TaskInfoAggregationResult<R>> result = SettableFuture.create();
    private volatile List<SerializedTaskInfo> driverTaskInfos = ImmutableList.of();
    private volatile boolean sealRequested;
    private volatile long sealRequestedNanos;
    private volatile Thread thread;
    @GuardedBy("this")
    private TaskInfoAggregationResult<R> sealResult;

    public TaskInfoAggregator(
            QueryId queryId,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            Codec<TaskInfo> taskInfoCodec,
            TaskInfoSink<R> sink,
            Duration drainInterval,
            Duration sealTimeout,
            DataSize maxBacklogSize)
    {
        this(queryId, taskInfoCollector, createDecoder(taskInfoCodec), sink, drainInterval, sealTimeout, maxBacklogSize);
    }

    @VisibleForTesting
    TaskInfoAggregator(
            QueryId queryId,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            Function<byte[], TaskInfo> decoder,
            TaskInfoSink<R> sink,
            Duration drainInterval,
            Duration sealTimeout,
            DataSize maxBacklogSize)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.taskInfoCollector = requireNonNull(taskInfoCollector, "taskInfoCollector is null");
        this.decoder = requireNonNull(decoder, "decoder is null");
        this.sink = requireNonNull(sink, "sink is null");
        this.drainIntervalNanos = max(requireNonNull(drainInterval, "drainInterval is null").roundTo(NANOSECONDS), MILLISECONDS.toNanos(1));
        this.sealTimeoutMillis = requireNonNull(sealTimeout, "sealTimeout is null").toMillis();
        this.backlogLimitBytes = requireNonNull(maxBacklogSize, "maxBacklogSize is null").toBytes();
    }

    private static Function<byte[], TaskInfo> createDecoder(Codec<TaskInfo> taskInfoCodec)
    {
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        return compressedTaskInfo -> deserializeZstdCompressed(taskInfoCodec, compressedTaskInfo);
    }

    /**
     * Draining the accumulator with {@code value()} followed by {@code reset()} under its monitor is only atomic if
     * {@code add} and {@code merge} lock the accumulator itself, which they do from Spark 3.2 on (SPARK-20977).
     * Earlier versions lock the backing list instead, so a task info merged between the two calls would be lost.
     * Version strings that cannot be parsed are not supported.
     */
    public static boolean isIncrementalAggregationSupported(String sparkVersion)
    {
        List<String> parts = Splitter.on('.').limit(3).splitToList(sparkVersion);
        if (parts.size() < 2) {
            return false;
        }
        Integer majorVersion = Ints.tryParse(parts.get(0));
        Integer minorVersion = Ints.tryParse(parts.get(1));
        if (majorVersion == null || minorVersion == null) {
            return false;
        }
        return majorVersion > 3 || (majorVersion == 3 && minorVersion >= 2);
    }

    public static TaskInfoAggregationMode resolveMode(TaskInfoAggregationMode requestedMode, String sparkVersion)
    {
        if (requestedMode == INCREMENTAL && !isIncrementalAggregationSupported(sparkVersion)) {
            if (unsupportedSparkVersionLogged.compareAndSet(false, true)) {
                log.warn("%s task info aggregation requires Spark 3.2 or later but Spark %s is running, using %s task info aggregation", INCREMENTAL, sparkVersion, LEGACY);
            }
            return LEGACY;
        }
        return requestedMode;
    }

    /**
     * Returns a task info collector that can be drained atomically on any Spark version. For testing only.
     */
    public static CollectionAccumulator<SerializedTaskInfo> createLockingTaskInfoCollectorForTesting()
    {
        return new LockingCollectionAccumulator<>();
    }

    public void start()
    {
        if (closed.get() || !started.compareAndSet(false, true)) {
            return;
        }
        try {
            Thread thread = new Thread(new Worker(), "presto-spark-task-info-aggregator-" + queryId);
            thread.setDaemon(true);
            thread.setContextClassLoader(TaskInfoAggregator.class.getClassLoader());
            this.thread = thread;
            thread.start();
        }
        catch (Throwable t) {
            log.warn(t, "Failed to start the task info aggregator of query %s", queryId);
            result.set(TaskInfoAggregationResult.incomplete(true, false, 0));
        }
    }

    /**
     * Drains the task infos received so far, folds them together with the pending attempts of the tasks that never
     * finished, and returns the result. Waits for at most the seal timeout. Repeated calls return the same result.
     */
    public TaskInfoAggregationResult<R> seal()
    {
        return seal(ImmutableList.of());
    }

    /**
     * Like {@link #seal()}, and also folds {@code driverTaskInfos}, the task infos of the tasks that ran on the driver,
     * after the last drain. They are never dropped, and their bytes are left in place.
     */
    public synchronized TaskInfoAggregationResult<R> seal(List<SerializedTaskInfo> driverTaskInfos)
    {
        if (sealResult == null) {
            sealResult = awaitResult(ImmutableList.copyOf(driverTaskInfos));
            if (sealResult.isComplete()) {
                log.info("Task info aggregation of query %s: %s", queryId, sealResult);
            }
            else {
                log.warn("Task info aggregation of query %s is incomplete: %s", queryId, sealResult);
            }
        }
        return sealResult;
    }

    private TaskInfoAggregationResult<R> awaitResult(List<SerializedTaskInfo> driverTaskInfos)
    {
        if (!started.get()) {
            return TaskInfoAggregationResult.incomplete(true, false, 0);
        }
        long start = System.nanoTime();
        this.driverTaskInfos = driverTaskInfos;
        sealRequestedNanos = start;
        sealRequested = true;
        wakeUp.countDown();
        try {
            return result.get(sealTimeoutMillis, MILLISECONDS);
        }
        catch (TimeoutException e) {
            return TaskInfoAggregationResult.incomplete(false, true, System.nanoTime() - start);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskInfoAggregationResult.incomplete(true, false, System.nanoTime() - start);
        }
        catch (ExecutionException | RuntimeException e) {
            log.warn(e, "Failed to get the task info aggregation result of query %s", queryId);
            return TaskInfoAggregationResult.incomplete(true, false, System.nanoTime() - start);
        }
    }

    /**
     * Stops the aggregator thread and drops the task infos it has not drained.
     */
    @Override
    public void close()
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        wakeUp.countDown();
        Thread thread = this.thread;
        if (thread == null) {
            return;
        }
        try {
            thread.join(CLOSE_TIMEOUT_MILLIS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) {
            log.warn("Task info aggregator of query %s did not stop within %sms", queryId, CLOSE_TIMEOUT_MILLIS);
        }
        try {
            int undrainedCount;
            synchronized (taskInfoCollector) {
                undrainedCount = taskInfoCollector.value().size();
                taskInfoCollector.reset();
            }
            if (undrainedCount > 0) {
                log.info("Dropped %s undrained task infos of query %s", undrainedCount, queryId);
            }
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to drop the undrained task infos of query %s", queryId);
        }
    }

    private final class Worker
            implements Runnable
    {
        private final TaskInfoDeduplicator deduplicator = new TaskInfoDeduplicator();
        private final ArrayDeque<byte[]> backlog = new ArrayDeque<>();
        private long backlogBytes;
        private long peakBacklogBytes;
        private long receivedCount;
        private long foldedCount;
        private long pendingFoldedAtSealCount;
        private long foldFailureCount;
        private long droppedCount;
        private long droppedBytes;
        private long decodeTimeNanos;
        private long maxFoldLagMillis;
        private long totalFoldLagMillis;
        private long foldLagCount;

        @Override
        public void run()
        {
            TaskInfoAggregationResult<R> outcome = null;
            try {
                outcome = aggregate();
            }
            catch (Throwable t) {
                log.warn(t, "Task info aggregator of query %s failed", queryId);
                outcome = createResult(Optional.empty(), true);
            }
            finally {
                result.set(outcome == null ? TaskInfoAggregationResult.incomplete(true, false, 0) : outcome);
            }
        }

        private TaskInfoAggregationResult<R> aggregate()
                throws InterruptedException
        {
            while (!closed.get()) {
                // read before draining, so that the last drain sees every task info merged before the seal
                boolean sealing = sealRequested;
                drain();
                if (sealing) {
                    processBacklog();
                    processDriverTaskInfos();
                    foldPendingAttempts();
                    if (closed.get()) {
                        break;
                    }
                    Optional<R> sinkResult = sealSink();
                    return createResult(sinkResult, !sinkResult.isPresent());
                }
                long deadline = System.nanoTime() + drainIntervalNanos;
                processBacklog(deadline);
                long remainingNanos = deadline - System.nanoTime();
                if (backlog.isEmpty() && remainingNanos > 0) {
                    wakeUp.await(remainingNanos, NANOSECONDS);
                }
            }
            return createResult(Optional.empty(), true);
        }

        private void drain()
        {
            List<SerializedTaskInfo> taskInfos;
            // atomic only from Spark 3.2 on, see isIncrementalAggregationSupported
            synchronized (taskInfoCollector) {
                taskInfos = taskInfoCollector.value();
                if (!taskInfos.isEmpty()) {
                    taskInfoCollector.reset();
                }
            }
            for (SerializedTaskInfo taskInfo : taskInfos) {
                receivedCount++;
                try {
                    byte[] compressedTaskInfo = taskInfo.getBytesAndClear();
                    backlog.addLast(compressedTaskInfo);
                    backlogBytes += compressedTaskInfo.length;
                }
                catch (RuntimeException e) {
                    recordFoldFailure(e);
                }
            }
            while (backlogBytes + deduplicator.getPendingBytes() > backlogLimitBytes && !backlog.isEmpty()) {
                byte[] compressedTaskInfo = backlog.removeFirst();
                backlogBytes -= compressedTaskInfo.length;
                recordDropped(compressedTaskInfo);
            }
            peakBacklogBytes = max(peakBacklogBytes, backlogBytes + deduplicator.getPendingBytes());
        }

        private void processBacklog()
        {
            while (!backlog.isEmpty() && !closed.get()) {
                processNext();
            }
        }

        private void processBacklog(long deadlineNanos)
        {
            while (!backlog.isEmpty() && !closed.get() && System.nanoTime() - deadlineNanos < 0) {
                processNext();
            }
        }

        private void processNext()
        {
            byte[] compressedTaskInfo = backlog.removeFirst();
            backlogBytes -= compressedTaskInfo.length;
            process(compressedTaskInfo);
        }

        private void processDriverTaskInfos()
        {
            for (SerializedTaskInfo driverTaskInfo : driverTaskInfos) {
                if (closed.get()) {
                    return;
                }
                receivedCount++;
                try {
                    // not cleared, so that the caller can still publish the driver tasks if the aggregation fails
                    process(driverTaskInfo.getBytes());
                }
                catch (RuntimeException e) {
                    recordFoldFailure(e);
                }
            }
        }

        private void process(byte[] compressedTaskInfo)
        {
            try {
                TaskInfo taskInfo = decode(compressedTaskInfo);
                TaskInfoDeduplicator.Decision decision = deduplicator.offer(taskInfo, compressedTaskInfo);
                // driver task infos are not subject to the limit, so pending attempts can grow beyond it
                peakBacklogBytes = max(peakBacklogBytes, backlogBytes + deduplicator.getPendingBytes());
                if (decision == FOLD) {
                    long foldLagMillis = max(0, System.currentTimeMillis() - taskInfo.getLastHeartbeatInMillis());
                    fold(taskInfo);
                    maxFoldLagMillis = max(maxFoldLagMillis, foldLagMillis);
                    totalFoldLagMillis += foldLagMillis;
                    foldLagCount++;
                }
            }
            catch (RuntimeException e) {
                recordFoldFailure(e);
            }
        }

        private void foldPendingAttempts()
        {
            for (byte[] compressedTaskInfo : deduplicator.removePendingAttempts()) {
                if (closed.get()) {
                    return;
                }
                try {
                    fold(decode(compressedTaskInfo));
                    pendingFoldedAtSealCount++;
                }
                catch (RuntimeException e) {
                    recordFoldFailure(e);
                }
            }
        }

        private TaskInfo decode(byte[] compressedTaskInfo)
        {
            long start = System.nanoTime();
            try {
                return decoder.apply(compressedTaskInfo);
            }
            finally {
                decodeTimeNanos += System.nanoTime() - start;
            }
        }

        private void fold(TaskInfo taskInfo)
        {
            sink.addTask(getFragmentId(taskInfo), taskInfo);
            foldedCount++;
        }

        private Optional<R> sealSink()
        {
            try {
                return Optional.of(sink.seal());
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to seal the task info sink of query %s", queryId);
                return Optional.empty();
            }
        }

        private void recordDropped(byte[] compressedTaskInfo)
        {
            droppedCount++;
            droppedBytes += compressedTaskInfo.length;
        }

        private void recordFoldFailure(RuntimeException e)
        {
            foldFailureCount++;
            if (foldFailureCount <= MAX_LOGGED_FAILURES) {
                log.warn(e, "Failed to fold a task info of query %s", queryId);
            }
        }

        private TaskInfoAggregationResult<R> createResult(Optional<R> sinkResult, boolean degraded)
        {
            return new TaskInfoAggregationResult<>(
                    sinkResult,
                    degraded,
                    false,
                    receivedCount,
                    foldedCount,
                    deduplicator.getDuplicateFinishedCount(),
                    deduplicator.getSupersededCount(),
                    pendingFoldedAtSealCount,
                    foldFailureCount,
                    droppedCount,
                    droppedBytes,
                    peakBacklogBytes,
                    decodeTimeNanos,
                    maxFoldLagMillis,
                    foldLagCount == 0 ? 0 : totalFoldLagMillis / foldLagCount,
                    sealRequested ? System.nanoTime() - sealRequestedNanos : 0);
        }
    }

    // reproduces the locking of the Spark 3.2 and later CollectionAccumulator, so that INCREMENTAL can run on the Spark 2 test profile
    private static class LockingCollectionAccumulator<T>
            extends CollectionAccumulator<T>
    {
        @Override
        public CollectionAccumulator<T> copyAndReset()
        {
            return new CollectionAccumulator<>();
        }

        @Override
        public synchronized void add(T value)
        {
            super.add(value);
        }

        @Override
        public synchronized void merge(AccumulatorV2<T, List<T>> other)
        {
            super.merge(other);
        }

        @Override
        public synchronized List<T> value()
        {
            return super.value();
        }

        @Override
        public synchronized void reset()
        {
            super.reset();
        }
    }
}
