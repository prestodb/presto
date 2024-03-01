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
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.execution.TaskInfo;
import org.apache.commons.text.CaseUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.util.AccumulatorV2;

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
        if (taskInfo == null || taskInfo.getStats() == null) {
            return;
        }

        try {
            taskInfo.getStats().getRuntimeStats().getMetrics()
                    .forEach(PrestoSparkStatsCollectionUtils::incSparkInternalAccumulator);
        }
        catch (Exception e) {
            log.warn(e, "An error occurred while updating Spark Internal metrics for task=%s", taskInfo);
        }
    }

    static void incSparkInternalAccumulator(final String prestoKey, final RuntimeMetric metric)
    {
        TaskContext taskContext = TaskContext.get();
        if (taskContext == null) {
            return;
        }

        TaskMetrics sparkTaskMetrics = taskContext.taskMetrics();
        if (sparkTaskMetrics == null) {
            return;
        }

        String sparkInternalAccumulatorName = getSparkInternalAccumulatorKey(prestoKey);
        scala.Option accumulatorV2Optional = sparkTaskMetrics.nameToAccums().get(sparkInternalAccumulatorName);
        if (accumulatorV2Optional.isEmpty()) {
            return;
        }

        AccumulatorV2<Object, Object> accumulatorV2 = (AccumulatorV2<Object, Object>) accumulatorV2Optional.get();
        accumulatorV2.add(
                getMetricLongValue(metric, sparkInternalAccumulatorName.contains("Ms")));
    }

    static String getSparkInternalAccumulatorKey(final String prestoKey)
    {
        if (prestoKey.contains(SPARK_INTERNAL_ACCUMULATOR_PREFIX)) {
            int index = prestoKey.indexOf(PRESTO_NATIVE_OPERATOR_STATS_SEP);
            return prestoKey.substring(index);
        }
        String[] prestoKeyParts = prestoKey.split("\\.");
        int prestoKeyPartsLength = prestoKeyParts.length;
        if (prestoKeyPartsLength < 2) {
            log.debug("Fail to build spark internal key for %s format not supported", prestoKey);
            return "";
        }
        String prestoNewKey = String.format("%1$s%2$s", prestoKeyParts[0], prestoKeyParts[prestoKeyPartsLength - 1]);
        if (prestoNewKey.contains("_")) {
            prestoNewKey = CaseUtils.toCamelCase(prestoKey, false, '_');
        }
        return String.format("%1$s%2$s%3$s", SPARK_INTERNAL_ACCUMULATOR_PREFIX,
                PRESTO_NATIVE_OPERATOR_STATS_PREFIX, prestoNewKey);
    }

    static long getMetricLongValue(RuntimeMetric metric, boolean isSparkUnitMs)
    {
        long sum = metric.getSum();
        if (metric.getUnit().equals(RuntimeUnit.NANO) && isSparkUnitMs) {
            sum = TimeUnit.NANOSECONDS.toMillis(sum);
        }
        return sum;
    }
}
