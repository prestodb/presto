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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.NativeExecutionInfo;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import org.apache.commons.text.CaseUtils;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.util.AccumulatorV2;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class PrestoSparkStatsCollectionUtils
{
    public static final String SPARK_INTERNAL_ACCUMULATOR_PREFIX = "internal.metrics.";
    public static final String PRESTO_NATIVE_OPERATOR_STATS_SEP = "internal";
    public static final String PRESTO_NATIVE_OPERATOR_STATS_PREFIX = "velox.";
    private static final Logger log = Logger.get(PrestoSparkStatsCollectionUtils.class);

    private PrestoSparkStatsCollectionUtils() {}

    public static void collectMetrics(final TaskInfo taskInfo)
    {
        int taskId = -1;
        int stageId = -1;
        try {
            taskId = taskInfo.getTaskId().getId();
            stageId = taskInfo.getTaskId().getStageExecutionId().getStageId().getId();
            Set<RuntimeStats> runtimeStatsSet = collectRuntimeStats(taskInfo);
            collectMetrics(runtimeStatsSet);
        }
        catch (Exception e) {
            log.error("An error occurred while processing taskId=%s  stageId=%s", taskId, stageId, e);
        }
    }

    public static void collectMetrics(Set<RuntimeStats> runtimeStatsSet)
    {
        runtimeStatsSet.forEach(runStats ->
        {
            runStats.getMetrics().entrySet().forEach(entry -> {
                String prestoKey = entry.getKey();
                String sparkInternalAccumulatorKey = getSparkInternalAccumulatorKey(prestoKey);
                collectMetric(sparkInternalAccumulatorKey, prestoKey, entry.getValue());
            });
        });
    }

    static void collectMetric(final String sparkInternalAccumulatorKey,
            final String prestoKey,
            final RuntimeMetric metric)
    {
        boolean isSparkUnitMs = sparkInternalAccumulatorKey.contains("Ms");
        long metricVal = getMetricValue(metric, isSparkUnitMs);
        incSparkInternalAccumulator(sparkInternalAccumulatorKey, prestoKey, metricVal);
    }

    static String getSparkInternalAccumulatorKey(final String prestoKey)
    {
        if (prestoKey.contains(SPARK_INTERNAL_ACCUMULATOR_PREFIX)) {
            int index = prestoKey.indexOf(PRESTO_NATIVE_OPERATOR_STATS_SEP);
            return prestoKey.substring(index);
        }
        String[] strs = prestoKey.split("\\.");
        if (strs == null || strs.length < 2) {
            log.debug("Fail to build spark internal key for %s format not supported", prestoKey);
            return "";
        }
        String prestoNewKey = String.format("%1$s%2$s", strs[0], strs[strs.length - 1]);
        if (prestoNewKey.contains("_")) {
            prestoNewKey = CaseUtils.toCamelCase(prestoKey, false, '_');
        }
        return String.format("%1$s%2$s%3$s", SPARK_INTERNAL_ACCUMULATOR_PREFIX,
                PRESTO_NATIVE_OPERATOR_STATS_PREFIX, prestoNewKey);
    }

    static Set<RuntimeStats> collectRuntimeStats(TaskInfo taskInfo)
    {
        Set<RuntimeStats> stats = new HashSet<>();
        if (taskInfo.getStats() == null) {
            return stats;
        }
        for (PipelineStats pipelineStats : taskInfo.getStats().getPipelines()) {
            if (pipelineStats != null) {
                for (OperatorStats operatorStats : pipelineStats.getOperatorSummaries()) {
                    if (operatorStats != null) {
                        if (operatorStats.getOperatorType().equals("NativeExecutionOperator")) {
                            NativeExecutionInfo nativeExecutionInfo = (NativeExecutionInfo) operatorStats.getInfo();
                            if (nativeExecutionInfo != null) {
                                for (TaskStats taskStats : nativeExecutionInfo.getTaskStats()) {
                                    if (taskStats != null) {
                                        RuntimeStats runtimeStat = taskStats.getRuntimeStats();
                                        stats.add(runtimeStat);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return stats;
    }

    static long getMetricValue(RuntimeMetric metric, boolean isSparkUnitMs)
    {
        long sum = metric.getSum();
        if (metric.getUnit().equals(RuntimeUnit.NANO) && isSparkUnitMs) {
            sum = TimeUnit.NANOSECONDS.toMillis(sum);
        }
        return sum;
    }

    static void incSparkInternalAccumulator(final String sparkInternalAccuName, final String prestoKey, final Object metric)
    {
        TaskMetrics tm = org.apache.spark.TaskContext.get().taskMetrics();
        if (tm != null) {
            scala.Option acc2 = tm.nameToAccums().get(sparkInternalAccuName);
            if (!acc2.isEmpty()) {
                AccumulatorV2<Object, Object> acc = (AccumulatorV2<Object, Object>) acc2.get();
                acc.add(metric);
            }
            else {
                log.debug("Fail to find spark internal accumulator matching key:" +
                        " %s prestoKey = %s ", sparkInternalAccuName, prestoKey);
            }
        }
    }
}
