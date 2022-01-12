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
package com.facebook.presto.spark;

import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.spark.MapOutputStatistics;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.spark.util.PrestoSparkUtils.getActionResultWithTimeout;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RddAndMore<T extends PrestoSparkTaskOutput>
{
    private final JavaPairRDD<MutablePartitionId, T> rdd;
    private final List<PrestoSparkBroadcastDependency<?>> broadcastDependencies;

    private boolean collected;

    public RddAndMore(
            JavaPairRDD<MutablePartitionId, T> rdd,
            List<PrestoSparkBroadcastDependency<?>> broadcastDependencies)
    {
        this.rdd = requireNonNull(rdd, "rdd is null");
        this.broadcastDependencies = ImmutableList.copyOf(requireNonNull(broadcastDependencies, "broadcastDependencies is null"));
    }

    public List<Tuple2<MutablePartitionId, T>> collectAndDestroyDependenciesWithTimeout(long timeout, TimeUnit timeUnit, Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics)
            throws SparkException, TimeoutException
    {
        checkState(!collected, "already collected");
        collected = true;
        List<Tuple2<MutablePartitionId, T>> result = getActionResultWithTimeout(rdd.collectAsync(), timeout, timeUnit, waitTimeMetrics);
        broadcastDependencies.forEach(PrestoSparkBroadcastDependency::destroy);
        return result;
    }

    public List<Tuple2<MutablePartitionId, T>> collectAndDestroyDependencies(long timeout, TimeUnit timeUnit, Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics, SparkContext sparkContext)
            throws SparkException, TimeoutException
    {
        checkState(!collected, "already collected");
        collected = true;
        RDD<Tuple2<MutablePartitionId, T>> rootRDD = rdd.rdd();

        // Get the leaf RDD
        ShuffledRDD shuffledRDD = (ShuffledRDD) rootRDD.getDependencies().head().rdd();
        MapOutputStatistics mapOutputStatistics = executeShuffleDependency(sparkContext, (ShuffleDependency) shuffledRDD.getDependencies().head());
        long totalStageOutputBytes = Arrays.stream(mapOutputStatistics.bytesByPartitionId()).sum();

        // Compute the shuffle partitions based on output bytes
        int numberOfShufflePartitions = Math.min(1, (int) (totalStageOutputBytes / DataSize.succinctDataSize(1, DataSize.Unit.GIGABYTE).toBytes()));

        List<Tuple2<MutablePartitionId, T>> result = getActionResultWithTimeout(rdd.collectAsync(), timeout, timeUnit, waitTimeMetrics);
        broadcastDependencies.forEach(PrestoSparkBroadcastDependency::destroy);
        return result;
    }

    private static MapOutputStatistics executeShuffleDependency(SparkContext sparkContext, ShuffleDependency shuffleDependency)
            throws SparkException
    {
        return (MapOutputStatistics) sparkContext.submitMapStage(shuffleDependency).get();
    }

    public JavaPairRDD<MutablePartitionId, T> getRdd()
    {
        return rdd;
    }

    public List<PrestoSparkBroadcastDependency<?>> getBroadcastDependencies()
    {
        return broadcastDependencies;
    }

    public static MapOutputStatistics executeShuffleRDD(SparkContext sparkContext, ShuffledRDD shuffledRDD)
            throws SparkException
    {
        return executeShuffleDependency(sparkContext, (ShuffleDependency) shuffledRDD.getDependencies().head());
    }
}
