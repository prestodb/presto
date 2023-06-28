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
import org.apache.spark.Dependency;
import org.apache.spark.NarrowDependency;
import org.apache.spark.OneToOneDependency;
import org.apache.spark.Partition;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;
import org.apache.spark.rdd.ShuffledRDDPartition;
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
import java.util.stream.IntStream;

import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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
public class PrestoSparkNativeTaskRdd<T extends PrestoSparkTaskOutput>
        extends RDD<Tuple2<MutablePartitionId, T>>
{
    private List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds;
    private List<String> shuffleInputFragmentIds;
    private PrestoSparkTaskSourceRdd taskSourceRdd;
    private PrestoSparkTaskProcessor<T> taskProcessor;

    public static <T extends PrestoSparkTaskOutput> PrestoSparkNativeTaskRdd<T> create(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            // fragmentId -> RDD
            Map<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRddMap,
            PrestoSparkTaskProcessor<T> taskProcessor)
    {
        requireNonNull(context, "context is null");
        taskSourceRdd = requireNonNull(taskSourceRdd, "taskSourceRdd is null");
        shuffleInputRddMap = requireNonNull(shuffleInputRddMap, "shuffleInputRddMap is null");
        taskProcessor = requireNonNull(taskProcessor, "taskProcessor is null");
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
        Map<Integer, List<Partition>> partitionsByRDD = ((PrestoSparkZippedPartition) split).partitions();
        Iterator<SerializedPrestoSparkTaskSource> taskSourceIterator;
        if (taskSourceRdd != null) {
            checkArgument(partitionsByRDD.get(taskSourceRdd.id()).size() == 1, "TaskSourceRdd should only have 1 partition");
            taskSourceIterator = taskSourceRdd.iterator(partitionsByRDD.get(taskSourceRdd.id()).get(0), context);
        }
        else {
            taskSourceIterator = emptyScalaIterator();
        }

        return getTaskProcessor().process(
                taskSourceIterator,
                getShuffleReadDescriptors(partitionsByRDD),
                getShuffleWriteDescriptor(split));
    }

    private static Seq<?> getRDDSequence(Optional<PrestoSparkTaskSourceRdd> taskSourceRdd, List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds)
    {
        List<RDD<?>> list = new ArrayList<>(shuffleInputRdds);
        taskSourceRdd.ifPresent(list::add);

        return asScalaBuffer(list.stream().map(shuffleInputRdd -> {
            if (shuffleInputRdd.name().contains("[BROADCAST]")) {
                return new OneToAllDependency<>(shuffleInputRdd);
            }
            return new OneToOneDependency<>(shuffleInputRdd);
        }).collect(Collectors.toList())).toSeq();
    }

    @Override
    public Partition[] getPartitions()
    {
        Optional<Dependency<?>> d = seqAsJavaList(dependencies()).stream().filter(dep -> dep instanceof OneToOneDependency).findFirst();

        checkArgument(d.isPresent(), "No OneToOne dependency found");

        return IntStream.range(0, d.get().rdd().getNumPartitions())
                .mapToObj(partitionId -> new PrestoSparkZippedPartition(partitionId, dependencies()))
                .toArray(Partition[]::new);
    }

    private static class PrestoSparkZippedPartition
            implements Partition
    {
        private final int index;
        private final Map<Integer, List<Partition>> zippedPartitions;

        public PrestoSparkZippedPartition(int index, Seq<Dependency<?>> dependencies)
        {
            this.index = index;
            this.zippedPartitions = new HashMap<>();
            seqAsJavaList(dependencies).forEach(dependency -> {
                RDD<?> rdd = dependency.rdd();
                checkArgument(dependency instanceof NarrowDependency, "ZippedPartitions can only process Narrow dependencies");
                NarrowDependency<?> d = (NarrowDependency<?>) dependency;

                ImmutableList.Builder<Partition> partitionsForRDD = ImmutableList.builder();
                seqAsJavaList(d.getParents(index)).stream().mapToInt(obj -> (Integer) obj).forEach(parentPartitionId -> {
                    partitionsForRDD.add(rdd.partitions()[parentPartitionId]);
                });

                zippedPartitions.put(rdd.id(), partitionsForRDD.build());
            });
        }

        @Override
        public int index()
        {
            return index;
        }

        public Map<Integer, List<Partition>> partitions()
        {
            return zippedPartitions;
        }
    }

    private PrestoSparkNativeTaskRdd(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            List<String> shuffleInputFragmentIds,
            List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds,
            PrestoSparkTaskProcessor<T> taskProcessor)
    {
        super(context, ((Seq<Dependency<?>>) getRDDSequence(taskSourceRdd, shuffleInputRdds)), fakeClassTag());
        this.shuffleInputFragmentIds = shuffleInputFragmentIds;
        this.shuffleInputRdds = shuffleInputRdds;
        // Optional is not Java Serializable
        this.taskSourceRdd = taskSourceRdd.orElse(null);
        this.taskProcessor = context.clean(taskProcessor, true);
    }

    private static <T> ClassTag<T> fakeClassTag()
    {
        return scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
    }

    private Map<String, PrestoSparkShuffleReadDescriptor> getShuffleReadDescriptors(Map<Integer, List<Partition>> partitionsByRDD)
    {
        //The classloader_interface package tries to have minimal external dependencies (except the spark-core), so we use HashMap instead of Guava's ImmutableMap
        ImmutableMap.Builder<String, PrestoSparkShuffleReadDescriptor> shuffleReadDescriptors = ImmutableMap.builder();

        // Get shuffle information from ShuffledRdds for shuffle read
        List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds = getShuffleInputRdds();
        List<String> shuffleInputFragmentIds = getShuffleInputFragmentIds();

        for (int i = 0; i < shuffleInputRdds.size(); i++) {
            RDD<?> shuffleRdd = shuffleInputRdds.get(i);
            checkState(shuffleRdd != null);
            checkState(shuffleRdd instanceof ShuffledRDD, "ShuffledRdd is required but got: %s", shuffleRdd.getClass().getName());

            List<Partition> partitions = partitionsByRDD.get(shuffleRdd.id());

            ShuffleHandle handle = ((ShuffleDependency<?, ?, ?>) shuffleRdd.dependencies().head()).shuffleHandle();
            checkState(partitions != null && partitions.size() > 0, "ShuffleRDD should have atleast 1 partition");
            checkState(partitions.get(0) instanceof ShuffledRDDPartition,
                    "partition is required to be ShuffledRddPartition, but got: %s", partitions.get(0).getClass().getName());
            shuffleReadDescriptors.put(
                    shuffleInputFragmentIds.get(i),
                    new PrestoSparkShuffleReadDescriptor(
                            partitions,
                            handle,
                            shuffleRdd.getNumPartitions(),
                            getBlockIds(partitions, handle),
                            getPartitionIds(partitions, handle),
                            getPartitionSize(partitions, handle)));
        }
        return shuffleReadDescriptors.build();
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

    public PrestoSparkTaskProcessor<T> getTaskProcessor()
    {
        return taskProcessor;
    }

    private static class OneToAllDependency<T>
            extends NarrowDependency<T>
    {
        private final Integer numPartitions;

        public OneToAllDependency(RDD<T> parentRdd)
        {
            super(parentRdd);
            this.numPartitions = parentRdd.getNumPartitions();
        }

        @Override
        public Seq<Object> getParents(int partitionId)
        {
            // For any partition, this will return all the partitions of the
            // parent stage, this is applicable if the parent stage has been broadcasted
            return asScalaBuffer(IntStream.range(0, numPartitions).mapToObj(pid -> (Object) pid).collect(Collectors.toList()));
        }
    }

    private Optional<PrestoSparkShuffleWriteDescriptor> getShuffleWriteDescriptor(Partition split)
    {
        // Get shuffle information from Spark shuffle manager for shuffle write
        checkState(
                SparkEnv.get().shuffleManager() instanceof PrestoSparkNativeExecutionShuffleManager,
                "Native execution requires to use PrestoSparkNativeExecutionShuffleManager. But got: %s", SparkEnv.get().shuffleManager().getClass().getName());
        PrestoSparkNativeExecutionShuffleManager shuffleManager = (PrestoSparkNativeExecutionShuffleManager) SparkEnv.get().shuffleManager();
        Optional<ShuffleHandle> shuffleHandle = shuffleManager.getShuffleHandle(split.index());

        return shuffleHandle.map(handle -> new PrestoSparkShuffleWriteDescriptor(handle, shuffleManager.getNumOfPartitions(handle.shuffleId())));
    }

    private List<String> getBlockIds(List<Partition> partitions, ShuffleHandle shuffleHandle)
    {
        return getMapSizes(partitions, shuffleHandle).stream().map(item -> item._1.executorId()).collect(Collectors.toList());
    }

    private List<String> getPartitionIds(List<Partition> partitions, ShuffleHandle shuffleHandle)
    {
        return getMapSizes(partitions, shuffleHandle).stream()
                .map(item -> asJavaCollection(item._2))
                .flatMap(Collection::stream)
                .map(i -> i._1.toString())
                .collect(Collectors.toList());
    }

    private List<Long> getPartitionSize(List<Partition> partitions, ShuffleHandle shuffleHandle)
    {
        //Each partition/BlockManagerId can contain multiple blocks (with BlockId), here sums up all the blocks from each BlockManagerId/Partition
        return getMapSizes(partitions, shuffleHandle).stream()
                .map(
                        item -> seqAsJavaList(item._2)
                                .stream()
                                .map(item2 -> ((Long) item2._2))
                                .reduce(0L, Long::sum))
                .collect(Collectors.toList());
    }

    private Collection<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> getMapSizes(List<Partition> partitions, ShuffleHandle shuffleHandle)
    {
        int start = partitions.get(0).index();
        return asJavaCollection(SparkEnv.get().mapOutputTracker().getMapSizesByExecutorId(
                shuffleHandle.shuffleId(), start, start + partitions.size()));
    }
}
