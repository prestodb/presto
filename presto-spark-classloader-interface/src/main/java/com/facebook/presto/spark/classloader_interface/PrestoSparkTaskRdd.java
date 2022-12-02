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

import org.apache.spark.MapOutputTracker;
import org.apache.spark.Partition;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import org.apache.spark.rdd.ShuffledRDDPartition;
import org.apache.spark.rdd.ZippedPartitionsBaseRDD;
import org.apache.spark.rdd.ZippedPartitionsPartition;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static scala.collection.JavaConversions.asJavaCollection;
import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.seqAsJavaList;

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
public class PrestoSparkTaskRdd<T extends PrestoSparkTaskOutput>
        extends ZippedPartitionsBaseRDD<Tuple2<MutablePartitionId, T>>
{
    private final boolean isNativeExecutionEnabled;
    private List<String> shuffleInputFragmentIds;
    private List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds;
    private PrestoSparkTaskSourceRdd taskSourceRdd;
    private PrestoSparkTaskProcessor<T> taskProcessor;

    public static <T extends PrestoSparkTaskOutput> PrestoSparkTaskRdd<T> create(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            // fragmentId -> RDD
            Map<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRddMap,
            PrestoSparkTaskProcessor<T> taskProcessor,
            boolean isNativeExecutionEnabled)
    {
        requireNonNull(context, "context is null");
        requireNonNull(taskSourceRdd, "taskSourceRdd is null");
        requireNonNull(shuffleInputRddMap, "shuffleInputRdds is null");
        requireNonNull(taskProcessor, "taskProcessor is null");
        List<String> shuffleInputFragmentIds = new ArrayList<>();
        List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds = new ArrayList<>();
        for (Map.Entry<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> entry : shuffleInputRddMap.entrySet()) {
            shuffleInputFragmentIds.add(entry.getKey());
            shuffleInputRdds.add(entry.getValue());
        }
        return new PrestoSparkTaskRdd<>(context, taskSourceRdd, shuffleInputFragmentIds, shuffleInputRdds, taskProcessor, isNativeExecutionEnabled);
    }

    private PrestoSparkTaskRdd(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            List<String> shuffleInputFragmentIds,
            List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds,
            PrestoSparkTaskProcessor<T> taskProcessor,
            boolean isNativeExecutionEnabled)
    {
        super(context, getRDDSequence(taskSourceRdd, shuffleInputRdds), false, fakeClassTag());
        this.shuffleInputFragmentIds = shuffleInputFragmentIds;
        this.shuffleInputRdds = shuffleInputRdds;
        // Optional is not Java Serializable
        this.taskSourceRdd = taskSourceRdd.orElse(null);
        this.taskProcessor = context.clean(taskProcessor, true);
        this.isNativeExecutionEnabled = isNativeExecutionEnabled;
    }

    private static Seq<RDD<?>> getRDDSequence(Optional<PrestoSparkTaskSourceRdd> taskSourceRdd, List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds)
    {
        List<RDD<?>> list = new ArrayList<>(shuffleInputRdds);
        taskSourceRdd.ifPresent(list::add);
        return asScalaBuffer(list).toSeq();
    }

    private static <T> ClassTag<T> fakeClassTag()
    {
        return scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
    }

    private void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new RuntimeException(message);
        }
    }

    private ShuffleHandle getShuffleHandle(RDD<?> rdd)
    {
        checkArgument(rdd instanceof ShuffledRDD, format("ShuffledRdd is required but got: %s", rdd.getClass().getName()));
        return ((ShuffleDependency<?, ?, ?>) rdd.dependencies().head()).shuffleHandle();
    }

    private int getNumberOfPartitions(RDD<?> rdd)
    {
        checkArgument(rdd instanceof ShuffledRDD, format("ShuffledRdd is required but got: %s", rdd.getClass().getName()));
        return rdd.getNumPartitions();
    }

    private List<String> getBlockIds(ShuffledRDDPartition partition, ShuffleHandle shuffleHandle)
    {
        MapOutputTracker mapOutputTracker = SparkEnv.get().mapOutputTracker();
        Collection<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> mapSizes = asJavaCollection(mapOutputTracker.getMapSizesByExecutorId(
                shuffleHandle.shuffleId(), partition.idx(), partition.idx() + 1));
        return mapSizes.stream().map(item -> item._1.executorId()).collect(Collectors.toList());
    }

    private List<Long> getPartitionSize(ShuffledRDDPartition partition, ShuffleHandle shuffleHandle)
    {
        MapOutputTracker mapOutputTracker = SparkEnv.get().mapOutputTracker();
        Collection<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> mapSizes = asJavaCollection(mapOutputTracker.getMapSizesByExecutorId(
                shuffleHandle.shuffleId(), partition.idx(), partition.idx() + 1));
        //Each partition/BlockManagerId can contain multiple blocks (with BlockId), here sums up all the blocks from each BlockManagerId/Partition
        return mapSizes.stream().map(item -> seqAsJavaList(item._2).stream().map(item2 -> ((Long) item2._2)).reduce(0L, Long::sum)).collect(Collectors.toList());
    }

    private Map<String, PrestoSparkShuffleReadDescriptor> getShuffleReadDescriptors(List<Partition> partitions)
    {
        //The classloader_interface package tries to have minimal external dependencies (except the spark-core), so we use HashMap instead of Guava's ImmutableMap
        Map<String, PrestoSparkShuffleReadDescriptor> shuffleReadDescriptors = new HashMap<>();

        if (!isNativeExecutionEnabled) {
            return shuffleReadDescriptors;
        }

        // Get shuffle information from ShuffledRdds for shuffle read
        for (int inputIndex = 0; inputIndex < shuffleInputRdds.size(); inputIndex++) {
            Partition partition = partitions.get(inputIndex);
            ShuffleHandle handle = getShuffleHandle(shuffleInputRdds.get(inputIndex));
            checkArgument(partition instanceof ShuffledRDDPartition, format("partition is required to be ShuffledRddPartition, but got: %s", partition.getClass().getName()));
            shuffleReadDescriptors.put(
                    shuffleInputFragmentIds.get(inputIndex),
                    new PrestoSparkShuffleReadDescriptor(
                            partition,
                            handle,
                            getNumberOfPartitions(shuffleInputRdds.get(inputIndex)),
                            getBlockIds(((ShuffledRDDPartition) partition), handle),
                            getPartitionSize(((ShuffledRDDPartition) partition), handle)));
        }

        return shuffleReadDescriptors;
    }

    private Optional<PrestoSparkShuffleWriteDescriptor> getShuffleWriteDescriptor(Partition split)
    {
        if (!isNativeExecutionEnabled) {
            return Optional.empty();
        }

        // Get shuffle information from Spark shuffle manager for shuffle write
        checkArgument(
                SparkEnv.get().shuffleManager() instanceof PrestoSparkNativeExecutionShuffleManager,
                format("Native execution requires to use PrestoSparkNativeExecutionShuffleManager. But got: %s", SparkEnv.get().shuffleManager().getClass().getName()));
        PrestoSparkNativeExecutionShuffleManager shuffleManager = (PrestoSparkNativeExecutionShuffleManager) SparkEnv.get().shuffleManager();
        Optional<ShuffleHandle> shuffleHandle = shuffleManager.getShuffleHandle(split.index());

        return shuffleHandle.map(handle -> new PrestoSparkShuffleWriteDescriptor(handle, shuffleManager.getNumOfPartitions(handle.shuffleId())));
    }

    @Override
    public Iterator<Tuple2<MutablePartitionId, T>> compute(Partition split, TaskContext context)
    {
        List<Partition> partitions = seqAsJavaList(((ZippedPartitionsPartition) split).partitions());
        int expectedPartitionsSize = (taskSourceRdd != null ? 1 : 0) + shuffleInputRdds.size();
        if (partitions.size() != expectedPartitionsSize) {
            throw new IllegalArgumentException(format("Unexpected partitions size. Expected: %s. Actual: %s.", expectedPartitionsSize, partitions.size()));
        }

        Map<String, Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputIterators = new HashMap<>();
        for (int inputIndex = 0; inputIndex < shuffleInputRdds.size(); inputIndex++) {
            shuffleInputIterators.put(
                    shuffleInputFragmentIds.get(inputIndex),
                    shuffleInputRdds.get(inputIndex).iterator(partitions.get(inputIndex), context));
        }

        Iterator<SerializedPrestoSparkTaskSource> taskSourceIterator;
        if (taskSourceRdd != null) {
            taskSourceIterator = taskSourceRdd.iterator(partitions.get(partitions.size() - 1), context);
        }
        else {
            taskSourceIterator = emptyScalaIterator();
        }

        return taskProcessor.process(
                taskSourceIterator,
                unmodifiableMap(shuffleInputIterators),
                getShuffleReadDescriptors(partitions),
                getShuffleWriteDescriptor(split),
                isNativeExecutionEnabled);
    }

    @Override
    public void clearDependencies()
    {
        super.clearDependencies();
        shuffleInputFragmentIds = null;
        shuffleInputRdds = null;
        taskSourceRdd = null;
        taskProcessor = null;
    }

    public List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> getShuffleInputRdds()
    {
        return shuffleInputRdds;
    }

    public PrestoSparkTaskSourceRdd getTaskSourceRdd()
    {
        return taskSourceRdd;
    }

    public List<String> getShuffleInputFragmentIds()
    {
        return shuffleInputFragmentIds;
    }
}
