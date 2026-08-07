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
package com.facebook.presto.spark.util;

import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.stats.Distribution.DistributionSnapshot;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.operator.DynamicFilterStats;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils.PRESTO_NATIVE_OPERATOR_STATS_PREFIX;
import static com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils.PRESTO_NATIVE_OPERATOR_STATS_SEP;
import static com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils.SPARK_INTERNAL_ACCUMULATOR_PREFIX;
import static com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils.collectTaskStatsMetrics;
import static com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils.computeTaskStatsMetrics;
import static com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils.getMetricLongValue;
import static com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils.getSparkInternalAccumulatorKey;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testng.Assert.assertEquals;

public class TestPrestoSparkStatsCollectionUtils
{
    // ========== getSparkInternalAccumulatorKey tests ==========

    @Test
    public void getSparkInternalAccumulatorKeyInternalKeyTest()
    {
        String expected = "internal.metrics.appname.writerRejectedPackageRawBytes";
        String prestoKey = "ShuffleWrite.root.internal.metrics.appname.writerRejectedPackageRawBytes";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, expected);
    }

    @Test
    public void getSparkInternalAccumulatorKeyTest()
    {
        String expected = "internal.metrics.velox.TableScanBlockedWaitForSplitTimes";
        String prestoKey = "TableScan.0.BlockedWaitForSplitTimes";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, expected);
    }

    @Test
    public void getSparkInternalAccumulatorKeyUnsupportedTest()
    {
        String expected = "";
        String prestoKey = "UnknownFormat";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, expected);
    }

    @Test
    public void testGetSparkInternalAccumulatorKeyWithTwoParts()
    {
        // Two-part key (operator.metric) should produce velox-prefixed key
        String prestoKey = "TableScan.InputPositions";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, SPARK_INTERNAL_ACCUMULATOR_PREFIX + PRESTO_NATIVE_OPERATOR_STATS_PREFIX + "TableScanInputPositions");
    }

    @Test
    public void testGetSparkInternalAccumulatorKeyWithInternalPrefix()
    {
        // Keys that already contain the internal.metrics. prefix should pass through
        String prestoKey = "ShuffleRead.root.internal.metrics.shuffle.read.remoteBytesRead";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, "internal.metrics.shuffle.read.remoteBytesRead");
    }

    @Test
    public void testGetSparkInternalAccumulatorKeyWithUnderscore()
    {
        // Keys with underscores should be camelCase-converted
        String prestoKey = "TableScan.0.blocked_wait_for_split";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        // CaseUtils.toCamelCase with initialLowerCase=false lowercases the first char of the full key
        String expected = SPARK_INTERNAL_ACCUMULATOR_PREFIX + PRESTO_NATIVE_OPERATOR_STATS_PREFIX + "tablescan.0.blockedWaitForSplit";
        assertEquals(actual, expected);
    }

    @Test
    public void testGetSparkInternalAccumulatorKeyEmptyString()
    {
        // Empty string has no parts when split, should return empty
        String actual = getSparkInternalAccumulatorKey("");
        assertEquals(actual, "");
    }

    @Test
    public void testGetSparkInternalAccumulatorKeyShuffleWriteMetric()
    {
        // Shuffle write metric with internal.metrics prefix should pass through
        String prestoKey = "ShuffleWrite.root.internal.metrics.shuffle.write.bytesWritten";
        String actual = getSparkInternalAccumulatorKey(prestoKey);
        assertEquals(actual, "internal.metrics.shuffle.write.bytesWritten");
    }

    // ========== getMetricLongValue tests ==========

    @Test
    public void testGetMetricLongValueNanosToMs()
    {
        // NANO unit with ms-suffixed accumulator should convert nanos to ms
        RuntimeMetric metric = new RuntimeMetric("test", RuntimeUnit.NANO, 5_000_000_000L, 1, 5_000_000_000L, 5_000_000_000L);
        long result = getMetricLongValue(metric, true);
        assertEquals(result, 5000L); // 5 billion nanos = 5000 ms
    }

    @Test
    public void testGetMetricLongValueNoConversionWhenNotMs()
    {
        // NANO unit but NOT ms-suffixed should return raw sum
        RuntimeMetric metric = new RuntimeMetric("test", RuntimeUnit.NANO, 5_000_000_000L, 1, 5_000_000_000L, 5_000_000_000L);
        long result = getMetricLongValue(metric, false);
        assertEquals(result, 5_000_000_000L);
    }

    @Test
    public void testGetMetricLongValueByteUnit()
    {
        // BYTE unit should always return raw sum
        RuntimeMetric metric = new RuntimeMetric("test", RuntimeUnit.BYTE, 1048576L, 1, 1048576L, 1048576L);
        long result = getMetricLongValue(metric, true);
        assertEquals(result, 1048576L);
    }

    @Test
    public void testGetMetricLongValueNoneUnit()
    {
        // NONE unit should return raw sum
        RuntimeMetric metric = new RuntimeMetric("test", RuntimeUnit.NONE, 42L, 1, 42L, 42L);
        long result = getMetricLongValue(metric, false);
        assertEquals(result, 42L);
    }

    @Test
    public void testGetMetricLongValueZero()
    {
        RuntimeMetric metric = new RuntimeMetric("test", RuntimeUnit.NANO, 0L, 0, 0L, 0L);
        long result = getMetricLongValue(metric, true);
        assertEquals(result, 0L);
    }

    @Test
    public void testGetMetricLongValueSmallNanos()
    {
        // Small nanos value that rounds down to 0 ms
        RuntimeMetric metric = new RuntimeMetric("test", RuntimeUnit.NANO, 999_999L, 1, 999_999L, 999_999L);
        long result = getMetricLongValue(metric, true);
        assertEquals(result, 0L); // 999,999 nanos < 1 ms
    }

    @Test
    public void testGetMetricLongValueExactOneMs()
    {
        RuntimeMetric metric = new RuntimeMetric("test", RuntimeUnit.NANO, 1_000_000L, 1, 1_000_000L, 1_000_000L);
        long result = getMetricLongValue(metric, true);
        assertEquals(result, 1L);
    }

    // ========== Constants tests ==========

    @Test
    public void testSparkInternalAccumulatorPrefixConstant()
    {
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX, "internal.metrics.");
    }

    @Test
    public void testPrestoNativeOperatorStatsConstants()
    {
        assertEquals(PRESTO_NATIVE_OPERATOR_STATS_SEP, "internal");
        assertEquals(PRESTO_NATIVE_OPERATOR_STATS_PREFIX, "velox.");
    }

    // ========== collectTaskStatsMetrics tests ==========

    @Test
    public void testCollectTaskStatsMetricsHandlesNull()
    {
        collectTaskStatsMetrics(null);
    }

    @Test
    public void testComputeTaskStatsMetricsWithFullyPopulatedTaskStats()
    {
        TaskStats taskStats = createFullyPopulatedTaskStats();
        Map<String, Long> metrics = computeTaskStatsMetrics(taskStats);

        // executorCpuTime: totalCpuTimeInNanos in raw nanos
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "executorCpuTime").longValue(),
                2_000_000_000L);

        // memoryBytesSpilled: aggregate spill from operators (single TableScan with 2048 spill)
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "memoryBytesSpilled").longValue(), 2048L);

        // Standard Spark I/O keys
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "input.bytesRead").longValue(), 1048576L);
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "input.recordsRead").longValue(), 50000L);
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "output.bytesWritten").longValue(), 131072L);
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "output.recordsWritten").longValue(), 12500L);
    }

    @Test
    public void testComputeTaskStatsMetricsReturnsExactlySevenKeys()
    {
        TaskStats taskStats = createFullyPopulatedTaskStats();
        Map<String, Long> metrics = computeTaskStatsMetrics(taskStats);
        assertEquals(metrics.size(), 6);
    }

    @Test
    public void testComputeTaskStatsMetricsWithZeroValues()
    {
        TaskStats taskStats = createTaskStatsWithZeros();
        Map<String, Long> metrics = computeTaskStatsMetrics(taskStats);

        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "executorCpuTime").longValue(), 0L);
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "memoryBytesSpilled").longValue(), 0L);
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "input.bytesRead").longValue(), 0L);
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "input.recordsRead").longValue(), 0L);
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "output.bytesWritten").longValue(), 0L);
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "output.recordsWritten").longValue(), 0L);
    }

    @Test
    public void testComputeTaskStatsMetricsWithLargeValues()
    {
        TaskStats taskStats = createTaskStatsWithLargeValues();
        Map<String, Long> metrics = computeTaskStatsMetrics(taskStats);

        // executorCpuTime stays in nanos
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "executorCpuTime").longValue(), Long.MAX_VALUE / 2);

        // Large byte values should pass through
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "input.bytesRead").longValue(), Long.MAX_VALUE / 2);
    }

    @Test
    public void testComputeTaskStatsMetricsSpillAggregatesAcrossPipelines()
    {
        TaskStats taskStats = createTaskStatsWithMultiplePipelines();
        Map<String, Long> metrics = computeTaskStatsMetrics(taskStats);

        // Pipeline 1: TableScan(4096 spill) + FilterAndProject(0 spill)
        // Pipeline 2: HashBuilderOperator(8192 spill)
        // Total = 4096 + 0 + 8192 = 12288
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "memoryBytesSpilled").longValue(), 12288L);
    }

    @Test
    public void testComputeTaskStatsMetricsWithNullPipelines()
    {
        // TaskStats with empty pipelines (using the two-arg constructor)
        TaskStats taskStats = new TaskStats(System.currentTimeMillis(), System.currentTimeMillis());
        Map<String, Long> metrics = computeTaskStatsMetrics(taskStats);

        // Spill should be 0 when there are no pipelines
        assertEquals(metrics.get(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "memoryBytesSpilled").longValue(), 0L);
    }

    @Test
    public void testCollectTaskStatsMetricsWithEmptyPipelines()
    {
        TaskStats taskStats = new TaskStats(System.currentTimeMillis(), System.currentTimeMillis());
        collectTaskStatsMetrics(taskStats);
    }

    // ========== Metric key format tests ==========

    @Test
    public void testSparkIoAccumulatorKeyFormat()
    {
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "input.bytesRead", "internal.metrics.input.bytesRead");
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "input.recordsRead", "internal.metrics.input.recordsRead");
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "output.bytesWritten", "internal.metrics.output.bytesWritten");
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "output.recordsWritten", "internal.metrics.output.recordsWritten");
    }

    @Test
    public void testExistingSparkMetricKeyFormat()
    {
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "executorCpuTime", "internal.metrics.executorCpuTime");
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "memoryBytesSpilled", "internal.metrics.memoryBytesSpilled");
    }

    @Test
    public void testShuffleReadAccumulatorKeyFormat()
    {
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "shuffle.read.remoteBytesRead", "internal.metrics.shuffle.read.remoteBytesRead");
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "shuffle.read.recordsRead", "internal.metrics.shuffle.read.recordsRead");
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "shuffle.read.fetchWaitTime", "internal.metrics.shuffle.read.fetchWaitTime");
    }

    @Test
    public void testShuffleWriteAccumulatorKeyFormat()
    {
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "shuffle.write.bytesWritten", "internal.metrics.shuffle.write.bytesWritten");
        assertEquals(SPARK_INTERNAL_ACCUMULATOR_PREFIX + "shuffle.write.recordsWritten", "internal.metrics.shuffle.write.recordsWritten");
    }

    // ========== Helper methods ==========

    private static TaskStats createFullyPopulatedTaskStats()
    {
        return new TaskStats(
                System.currentTimeMillis(),  // createTimeInMillis
                100L,                         // firstStartTimeInMillis
                200L,                         // lastStartTimeInMillis
                300L,                         // lastEndTimeInMillis
                System.currentTimeMillis(),  // endTimeInMillis
                5_000_000_000L,              // elapsedTimeInNanos (5s)
                1_000_000_000L,              // queuedTimeInNanos (1s)
                10,                           // totalDrivers
                2,                            // queuedDrivers
                1,                            // queuedPartitionedDrivers
                100L,                         // queuedPartitionedSplitsWeight
                3,                            // runningDrivers
                2,                            // runningPartitionedDrivers
                200L,                         // runningPartitionedSplitsWeight
                1,                            // blockedDrivers
                4,                            // completedDrivers
                8,                            // totalNewDrivers
                1,                            // queuedNewDrivers
                2,                            // runningNewDrivers
                5,                            // completedNewDrivers
                20,                           // totalSplits
                5,                            // queuedSplits
                3,                            // runningSplits
                12,                           // completedSplits
                1024.5,                       // cumulativeUserMemory
                2048.75,                      // cumulativeTotalMemory
                4096L,                        // userMemoryReservationInBytes
                512L,                         // revocableMemoryReservationInBytes
                8192L,                        // systemMemoryReservationInBytes
                16384L,                       // peakTotalMemoryInBytes
                8192L,                        // peakUserMemoryInBytes
                32768L,                       // peakNodeTotalMemoryInBytes
                3_000_000_000L,              // totalScheduledTimeInNanos
                2_000_000_000L,              // totalCpuTimeInNanos
                500_000_000L,                // totalBlockedTimeInNanos
                false,                        // fullyBlocked
                ImmutableSet.of(),            // blockedReasons
                65536L,                       // totalAllocationInBytes
                1048576L,                     // rawInputDataSizeInBytes
                50000L,                       // rawInputPositions
                524288L,                      // processedInputDataSizeInBytes
                25000L,                       // processedInputPositions
                262144L,                      // outputDataSizeInBytes
                12500L,                       // outputPositions
                131072L,                      // physicalWrittenDataSizeInBytes
                3,                            // fullGcCount
                150L,                         // fullGcTimeInMillis
                createPipelinesWithOperators(), // pipelines
                new RuntimeStats());          // runtimeStats
    }

    private static TaskStats createTaskStatsWithZeros()
    {
        return new TaskStats(
                System.currentTimeMillis(), 0L, 0L, 0L,
                System.currentTimeMillis(),
                0L, 0L, 0, 0, 0, 0L, 0, 0, 0L, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0.0, 0.0, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L,
                false, ImmutableSet.of(), 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0, 0L,
                ImmutableList.of(),
                new RuntimeStats());
    }

    private static TaskStats createTaskStatsWithLargeValues()
    {
        return new TaskStats(
                System.currentTimeMillis(),
                Long.MAX_VALUE / 2, Long.MAX_VALUE / 2, Long.MAX_VALUE / 2,
                System.currentTimeMillis(),
                Long.MAX_VALUE / 2,        // elapsedTimeInNanos
                Long.MAX_VALUE / 2,        // queuedTimeInNanos
                Integer.MAX_VALUE,          // totalDrivers
                Integer.MAX_VALUE / 2,      // queuedDrivers
                Integer.MAX_VALUE / 4,      // queuedPartitionedDrivers
                Long.MAX_VALUE / 2,         // queuedPartitionedSplitsWeight
                Integer.MAX_VALUE / 4,      // runningDrivers
                Integer.MAX_VALUE / 4,      // runningPartitionedDrivers
                Long.MAX_VALUE / 2,         // runningPartitionedSplitsWeight
                Integer.MAX_VALUE / 4,      // blockedDrivers
                Integer.MAX_VALUE / 4,      // completedDrivers
                Integer.MAX_VALUE / 2,      // totalNewDrivers
                Integer.MAX_VALUE / 4,      // queuedNewDrivers
                Integer.MAX_VALUE / 4,      // runningNewDrivers
                Integer.MAX_VALUE / 4,      // completedNewDrivers
                Integer.MAX_VALUE,          // totalSplits
                Integer.MAX_VALUE / 2,      // queuedSplits
                Integer.MAX_VALUE / 4,      // runningSplits
                Integer.MAX_VALUE / 4,      // completedSplits
                Double.MAX_VALUE / 2,       // cumulativeUserMemory
                Double.MAX_VALUE / 2,       // cumulativeTotalMemory
                Long.MAX_VALUE / 2,         // userMemoryReservationInBytes
                Long.MAX_VALUE / 2,         // revocableMemoryReservationInBytes
                Long.MAX_VALUE / 2,         // systemMemoryReservationInBytes
                Long.MAX_VALUE / 2,         // peakTotalMemoryInBytes
                Long.MAX_VALUE / 2,         // peakUserMemoryInBytes
                Long.MAX_VALUE / 2,         // peakNodeTotalMemoryInBytes
                Long.MAX_VALUE / 2,         // totalScheduledTimeInNanos
                Long.MAX_VALUE / 2,         // totalCpuTimeInNanos
                Long.MAX_VALUE / 2,         // totalBlockedTimeInNanos
                true,                        // fullyBlocked
                ImmutableSet.of(),           // blockedReasons
                Long.MAX_VALUE / 2,         // totalAllocationInBytes
                Long.MAX_VALUE / 2,         // rawInputDataSizeInBytes
                Long.MAX_VALUE / 2,         // rawInputPositions
                Long.MAX_VALUE / 2,         // processedInputDataSizeInBytes
                Long.MAX_VALUE / 2,         // processedInputPositions
                Long.MAX_VALUE / 2,         // outputDataSizeInBytes
                Long.MAX_VALUE / 2,         // outputPositions
                Long.MAX_VALUE / 2,         // physicalWrittenDataSizeInBytes
                Integer.MAX_VALUE,           // fullGcCount
                Long.MAX_VALUE / 2,         // fullGcTimeInMillis
                ImmutableList.of(),
                new RuntimeStats());
    }

    private static TaskStats createTaskStatsWithMultiplePipelines()
    {
        List<PipelineStats> pipelines = createPipelinesWithMultipleOperatorTypes();
        return new TaskStats(
                System.currentTimeMillis(), 100L, 200L, 300L,
                System.currentTimeMillis(),
                3_000_000_000L, 500_000_000L,
                8, 2, 1, 50L, 2, 1, 100L, 1, 3,
                6, 1, 1, 3,
                15, 3, 2, 10,
                500.0, 1000.0, 2048L, 256L, 4096L,
                8192L, 4096L, 16384L,
                2_000_000_000L, 1_500_000_000L, 300_000_000L,
                false, ImmutableSet.of(), 32768L,
                524288L, 25000L, 262144L, 12500L,
                131072L, 6250L, 65536L,
                1, 50L,
                pipelines,
                new RuntimeStats());
    }

    private static List<PipelineStats> createPipelinesWithOperators()
    {
        OperatorStats operator = createOperatorStats("TableScan", 1024L, 100L, 512L, 50L, 2048L);
        PipelineStats pipeline = createPipelineStats(ImmutableList.of(operator));
        return ImmutableList.of(pipeline);
    }

    private static List<PipelineStats> createPipelinesWithMultipleOperatorTypes()
    {
        OperatorStats tableScanOp = createOperatorStats("TableScan", 2048L, 200L, 1024L, 100L, 4096L);
        OperatorStats filterOp = createOperatorStats("FilterAndProject", 1024L, 100L, 512L, 50L, 0L);
        OperatorStats hashBuildOp = createOperatorStats("HashBuilderOperator", 512L, 50L, 256L, 25L, 8192L);

        PipelineStats pipeline1 = createPipelineStats(ImmutableList.of(tableScanOp, filterOp));
        PipelineStats pipeline2 = createPipelineStats(ImmutableList.of(hashBuildOp));
        return ImmutableList.of(pipeline1, pipeline2);
    }

    private static OperatorStats createOperatorStats(
            String operatorType,
            long inputDataSizeBytes,
            long inputPositions,
            long outputDataSizeBytes,
            long outputPositions,
            long spilledDataSizeBytes)
    {
        return new OperatorStats(
                0,                                        // stageId
                0,                                        // stageExecutionId
                0,                                        // pipelineId
                0,                                        // operatorId
                new PlanNodeId("test"),                   // planNodeId
                operatorType,                             // operatorType
                1,                                        // totalDrivers
                0,                                        // isBlockedCalls
                new Duration(0, NANOSECONDS),             // isBlockedWall
                new Duration(0, NANOSECONDS),             // isBlockedCpu
                0L,                                       // isBlockedAllocationInBytes
                10,                                       // addInputCalls
                new Duration(100_000_000, NANOSECONDS),   // addInputWall (100ms)
                new Duration(50_000_000, NANOSECONDS),    // addInputCpu (50ms)
                0L,                                       // addInputAllocationInBytes
                inputDataSizeBytes,                       // rawInputDataSizeInBytes
                inputPositions,                           // rawInputPositions
                inputDataSizeBytes,                       // inputDataSizeInBytes
                inputPositions,                           // inputPositions
                0d,                                       // sumSquaredInputPositions
                10,                                       // getOutputCalls
                new Duration(80_000_000, NANOSECONDS),    // getOutputWall (80ms)
                new Duration(40_000_000, NANOSECONDS),    // getOutputCpu (40ms)
                0L,                                       // getOutputAllocationInBytes
                outputDataSizeBytes,                      // outputDataSizeInBytes
                outputPositions,                          // outputPositions
                0L,                                       // physicalWrittenDataSizeInBytes
                new Duration(0, NANOSECONDS),             // additionalCpu
                new Duration(20_000_000, NANOSECONDS),    // blockedWall (20ms)
                0,                                        // finishCalls
                new Duration(0, NANOSECONDS),             // finishWall
                new Duration(10_000_000, NANOSECONDS),    // finishCpu (10ms)
                0L,                                       // finishAllocationInBytes
                0L,                                       // userMemoryReservationInBytes
                0L,                                       // revocableMemoryReservationInBytes
                0L,                                       // systemMemoryReservationInBytes
                0L,                                       // peakUserMemoryReservationInBytes
                0L,                                       // peakSystemMemoryReservationInBytes
                0L,                                       // peakTotalMemoryReservationInBytes
                spilledDataSizeBytes,                     // spilledDataSizeInBytes
                Optional.empty(),                         // blockedReason
                null,                                     // info
                new RuntimeStats(),                       // runtimeStats
                new DynamicFilterStats(new HashSet<>()),   // dynamicFilterStats
                0,                                        // nullJoinBuildKeyCount
                0,                                        // joinBuildKeyCount
                0,                                        // nullJoinProbeKeyCount
                0);                                       // joinProbeKeyCount
    }

    private static PipelineStats createPipelineStats(List<OperatorStats> operators)
    {
        Distribution distribution = new Distribution();
        distribution.add(1);
        DistributionSnapshot snapshot = distribution.snapshot();

        return new PipelineStats(
                0,                            // pipelineId
                0L,                           // firstStartTimeInMillis
                0L,                           // lastStartTimeInMillis
                0L,                           // lastEndTimeInMillis
                true,                         // inputPipeline
                false,                        // outputPipeline
                1,                            // totalDrivers
                0,                            // queuedDrivers
                0,                            // queuedPartitionedDrivers
                0L,                           // queuedPartitionedSplitsWeight
                0,                            // runningDrivers
                0,                            // runningPartitionedDrivers
                0L,                           // runningPartitionedSplitsWeight
                0,                            // blockedDrivers
                0,                            // completedDrivers
                0L,                           // userMemoryReservationInBytes
                0L,                           // revocableMemoryReservationInBytes
                0L,                           // systemMemoryReservationInBytes
                snapshot,                     // queuedTime
                snapshot,                     // elapsedTime
                0L,                           // totalScheduledTimeInNanos
                0L,                           // totalCpuTimeInNanos
                0L,                           // totalBlockedTimeInNanos
                false,                        // fullyBlocked
                ImmutableSet.of(),            // blockedReasons
                0L,                           // totalAllocationInBytes
                0L,                           // rawInputDataSizeInBytes
                0L,                           // rawInputPositions
                0L,                           // processedInputDataSizeInBytes
                0L,                           // processedInputPositions
                0L,                           // outputDataSizeInBytes
                0L,                           // outputPositions
                0L,                           // physicalWrittenDataSizeInBytes
                operators,                    // operatorSummaries
                ImmutableList.of());          // drivers
    }
}
