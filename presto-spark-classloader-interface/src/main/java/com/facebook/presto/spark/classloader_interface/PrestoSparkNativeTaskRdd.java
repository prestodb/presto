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
package com.facebook.presto.spark.classloader_interface;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.MapOutputTracker;
import org.apache.spark.Partition;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import org.apache.spark.rdd.ShuffledRDDPartition;
import org.apache.spark.rdd.ZippedPartitionsPartition;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spark.classloader_interface.PrestoSparkUtils.asJavaCollection;
import static com.facebook.presto.spark.classloader_interface.PrestoSparkUtils.getMapSizesByExecutorId;
import static com.facebook.presto.spark.classloader_interface.PrestoSparkUtils.seqAsJavaList;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * PrestoSparkTaskRdd represents execution of Presto stage, it contains:
 * - A list of shuffleInputRdds, each of the corresponding to a child stage.
 * - An optional taskSourceRdd, which represents ALL table scan inputs in this stage.
 * <p>
 * Table scan is present when joining a bucketed table with an unbucketed table, for example:
 * Join
 * /  \
 * Scan  Remote Source
 * <p>
 * In this case, bucket to Spark partition mapping has to be consistent with the Spark shuffle partition.
 * <p>
 * When the stage partitioning is SINGLE_DISTRIBUTION and the shuffleInputRdds is empty,
 * the taskSourceRdd is expected to be present and contain exactly one empty partition.
 * <p>
 * The broadcast inputs are encapsulated in taskProcessor.
 */
public class PrestoSparkNativeTaskRdd<T extends PrestoSparkTaskOutput>
        extends PrestoSparkTaskRdd<T>
{
    public static <T extends PrestoSparkTaskOutput> PrestoSparkNativeTaskRdd<T> create(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            // fragmentId -> RDD
            Map<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRddMap,
            PrestoSparkTaskProcessor<T> taskProcessor)
    {
        requireNonNull(context, "context is null");
        requireNonNull(taskSourceRdd, "taskSourceRdd is null");
        requireNonNull(shuffleInputRddMap, "shuffleInputRddMap is null");
        requireNonNull(taskProcessor, "taskProcessor is null");
        ImmutableList.Builder<String> shuffleInputFragmentIds = ImmutableList.builder();
        ImmutableList.Builder<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds = ImmutableList.builder();
        for (Map.Entry<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> entry : shuffleInputRddMap.entrySet()) {
            shuffleInputFragmentIds.add(entry.getKey());
            shuffleInputRdds.add(entry.getValue());
        }
        return new PrestoSparkNativeTaskRdd<>(context, taskSourceRdd, shuffleInputFragmentIds.build(), shuffleInputRdds.build(), taskProcessor);
    }

    @Override
    public Iterator<Tuple2<MutablePartitionId, T>> compute(Partition split, TaskContext context)
    {
        PrestoSparkTaskSourceRdd taskSourceRdd = getTaskSourceRdd();
        List<Partition> partitions = seqAsJavaList(((ZippedPartitionsPartition) split).partitions());
        int expectedPartitionsSize = (taskSourceRdd != null ? 1 : 0) + getShuffleInputRdds().size();
        checkState(partitions.size() == expectedPartitionsSize,
                format("Unexpected partitions size. Expected: %s. Actual: %s.", expectedPartitionsSize, partitions.size()));

        Iterator<SerializedPrestoSparkTaskSource> taskSourceIterator;
        if (taskSourceRdd != null) {
            taskSourceIterator = taskSourceRdd.iterator(partitions.get(partitions.size() - 1), context);
        }
        else {
            taskSourceIterator = emptyScalaIterator();
        }

        return getTaskProcessor().process(
                taskSourceIterator,
                getShuffleReadDescriptors(partitions),
                getShuffleWriteDescriptor(context.stageId(), split));
    }

    private PrestoSparkNativeTaskRdd(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            List<String> shuffleInputFragmentIds,
            List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds,
            PrestoSparkTaskProcessor<T> taskProcessor)
    {
        super(context, taskSourceRdd, shuffleInputFragmentIds, shuffleInputRdds, taskProcessor);
    }

    private Map<String, PrestoSparkShuffleReadDescriptor> getShuffleReadDescriptors(List<Partition> partitions)
    {
        //The classloader_interface package tries to have minimal external dependencies (except the spark-core), so we use HashMap instead of Guava's ImmutableMap
        ImmutableMap.Builder<String, PrestoSparkShuffleReadDescriptor> shuffleReadDescriptors = ImmutableMap.builder();

        // Get shuffle information from ShuffledRdds for shuffle read
        int numPartitions = partitions.size();
        List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds = getShuffleInputRdds();
        List<String> shuffleInputFragmentIds = getShuffleInputFragmentIds();
        checkState(
                numPartitions >= shuffleInputRdds.size() && numPartitions >= shuffleInputFragmentIds.size(),
                format(
                        "Size of shuffleInputRdds %d or shuffleInputFragmentIds %d is not equal to number of partitions %d",
                        shuffleInputRdds.size(),
                        shuffleInputFragmentIds.size(),
                        numPartitions));
        for (int i = 0; i < shuffleInputRdds.size(); i++) {
            Partition partition = partitions.get(i);
            checkState(partition != null);
            checkState(partition instanceof ShuffledRDDPartition,
                    "partition is required to be ShuffledRddPartition, but got: %s", partition.getClass().getName());

            RDD<?> shuffleRdd = shuffleInputRdds.get(i);
            checkState(shuffleRdd != null);
            checkState(shuffleRdd instanceof ShuffledRDD, "ShuffledRdd is required but got: %s", shuffleRdd.getClass().getName());

            ShuffleHandle handle = ((ShuffleDependency<?, ?, ?>) shuffleRdd.dependencies().head()).shuffleHandle();
            shuffleReadDescriptors.put(
                    shuffleInputFragmentIds.get(i),
                    new PrestoSparkShuffleReadDescriptor(
                            partition,
                            handle,
                            shuffleRdd.getNumPartitions(),
                            getBlockIds(((ShuffledRDDPartition) partition), handle),
                            getPartitionIds(((ShuffledRDDPartition) partition), handle),
                            getPartitionSize(((ShuffledRDDPartition) partition), handle)));
        }
        return shuffleReadDescriptors.build();
    }

    private Optional<PrestoSparkShuffleWriteDescriptor> getShuffleWriteDescriptor(int stageId, Partition split)
    {
        // Get shuffle information from Spark shuffle manager for shuffle write
        checkState(
                SparkEnv.get().shuffleManager() instanceof PrestoSparkNativeExecutionShuffleManager,
                "Native execution requires to use PrestoSparkNativeExecutionShuffleManager. But got: %s", SparkEnv.get().shuffleManager().getClass().getName());
        PrestoSparkNativeExecutionShuffleManager shuffleManager = (PrestoSparkNativeExecutionShuffleManager) SparkEnv.get().shuffleManager();
        Optional<ShuffleHandle> shuffleHandle = shuffleManager.getShuffleHandle(stageId, split.index());

        return shuffleHandle.map(handle -> new PrestoSparkShuffleWriteDescriptor(handle, shuffleManager.getNumOfPartitions(handle.shuffleId())));
    }

    private List<String> getBlockIds(ShuffledRDDPartition partition, ShuffleHandle shuffleHandle)
    {
        MapOutputTracker mapOutputTracker = SparkEnv.get().mapOutputTracker();
        Collection<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapSizes = getMapSizesByExecutorId(mapOutputTracker,
                shuffleHandle.shuffleId(), partition.idx(), partition.idx() + 1);
        return mapSizes.stream().map(item -> item._1.executorId()).collect(Collectors.toList());
    }

    private List<String> getPartitionIds(ShuffledRDDPartition partition, ShuffleHandle shuffleHandle)
    {
        MapOutputTracker mapOutputTracker = SparkEnv.get().mapOutputTracker();
        Collection<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapSizes = getMapSizesByExecutorId(mapOutputTracker,
                shuffleHandle.shuffleId(), partition.idx(), partition.idx() + 1);
        return mapSizes.stream()
                .map(item -> asJavaCollection(item._2))
                .flatMap(Collection::stream)
                .map(i -> i._1().toString())
                .collect(Collectors.toList());
    }

    private List<Long> getPartitionSize(ShuffledRDDPartition partition, ShuffleHandle shuffleHandle)
    {
        MapOutputTracker mapOutputTracker = SparkEnv.get().mapOutputTracker();
        Collection<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> mapSizes = getMapSizesByExecutorId(mapOutputTracker,
                shuffleHandle.shuffleId(), partition.idx(), partition.idx() + 1);
        //Each partition/BlockManagerId can contain multiple blocks (with BlockId), here sums up all the blocks from each BlockManagerId/Partition
        return mapSizes.stream()
                .map(
                        item -> seqAsJavaList(item._2)
                                .stream()
                                .map(item2 -> ((Long) item2._2()))
                                .reduce(0L, Long::sum))
                .collect(Collectors.toList());
    }
}
