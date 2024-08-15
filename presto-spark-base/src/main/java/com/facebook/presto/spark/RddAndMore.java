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
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.google.common.collect.ImmutableList;
import org.apache.spark.Dependency;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.facebook.presto.spark.util.PrestoSparkUtils.getActionResultWithTimeout;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RddAndMore<T extends PrestoSparkTaskOutput>
{
    private final JavaPairRDD<MutablePartitionId, T> rdd;
    private final List<PrestoSparkBroadcastDependency<?>> broadcastDependencies;
    private final Optional<PartitioningHandle> partitioningHandle;
    private boolean collected;

    public RddAndMore(
            JavaPairRDD<MutablePartitionId, T> rdd,
            List<PrestoSparkBroadcastDependency<?>> broadcastDependencies)
    {
        this(rdd, broadcastDependencies, Optional.empty());
    }

    public RddAndMore(
            JavaPairRDD<MutablePartitionId, T> rdd,
            List<PrestoSparkBroadcastDependency<?>> broadcastDependencies,
            Optional<PartitioningHandle> partitioningHandle)
    {
        this.rdd = requireNonNull(rdd, "rdd is null");
        this.broadcastDependencies = ImmutableList.copyOf(requireNonNull(broadcastDependencies, "broadcastDependencies is null"));
        this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
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

    public JavaPairRDD<MutablePartitionId, T> getRdd()
    {
        return rdd;
    }

    public List<PrestoSparkBroadcastDependency<?>> getBroadcastDependencies()
    {
        return broadcastDependencies;
    }

    // Returns shuffle dependencies of underlying RDD
    public List<ShuffleDependency> getShuffleDependencies()
    {
        RDD underlyingRdd = getRdd().rdd();
        Collection<Dependency> dependencies = JavaConverters.asJavaCollectionConverter(underlyingRdd.getDependencies()).asJavaCollection();
        return dependencies.stream()
                .filter(a -> a instanceof ShuffleDependency)
                .map(b -> (ShuffleDependency) b)
                .collect(Collectors.toList());
    }

    public Optional<PartitioningHandle> getPartitioningHandle()
    {
        return partitioningHandle;
    }

    public boolean isBroadcastDistribution()
    {
        return this.getPartitioningHandle().isPresent() && this.getPartitioningHandle().get().equals(FIXED_BROADCAST_DISTRIBUTION);
    }
}
