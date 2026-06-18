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

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ZippedPartitionsBaseRDD;
import org.apache.spark.rdd.ZippedPartitionsPartition;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spark.classloader_interface.PrestoSparkUtils.asScalaBuffer;
import static com.facebook.presto.spark.classloader_interface.PrestoSparkUtils.seqAsJavaList;
import static com.facebook.presto.spark.classloader_interface.PrestoSparkUtils.toImmutableSeq;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
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
public class PrestoSparkTaskRdd<T extends PrestoSparkTaskOutput>
        extends ZippedPartitionsBaseRDD<Tuple2<MutablePartitionId, T>>
{
    private List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds;
    private List<String> shuffleInputFragmentIds;
    private PrestoSparkTaskSourceRdd taskSourceRdd;
    private PrestoSparkTaskProcessor<T> taskProcessor;

    public static <T extends PrestoSparkTaskOutput> PrestoSparkTaskRdd<T> create(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            // fragmentId -> RDD
            Map<String, RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRddMap,
            PrestoSparkTaskProcessor<T> taskProcessor)
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
        return new PrestoSparkTaskRdd<>(context, taskSourceRdd, shuffleInputFragmentIds, shuffleInputRdds, taskProcessor);
    }

    protected PrestoSparkTaskRdd(
            SparkContext context,
            Optional<PrestoSparkTaskSourceRdd> taskSourceRdd,
            List<String> shuffleInputFragmentIds,
            List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds,
            PrestoSparkTaskProcessor<T> taskProcessor)
    {
        super(context, getRDDSequence(taskSourceRdd, shuffleInputRdds), false, fakeClassTag());
        this.shuffleInputFragmentIds = shuffleInputFragmentIds;
        this.shuffleInputRdds = shuffleInputRdds;
        // Optional is not Java Serializable
        this.taskSourceRdd = taskSourceRdd.orElse(null);
        this.taskProcessor = context.clean(taskProcessor, true);
    }

    private static Seq<RDD<?>> getRDDSequence(Optional<PrestoSparkTaskSourceRdd> taskSourceRdd, List<RDD<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputRdds)
    {
        List<RDD<?>> list = new ArrayList<>(shuffleInputRdds);
        taskSourceRdd.ifPresent(list::add);
        return toImmutableSeq(asScalaBuffer(list).toSeq());
    }

    private static <T> ClassTag<T> fakeClassTag()
    {
        return scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
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

        return taskProcessor.process(taskSourceIterator, unmodifiableMap(shuffleInputIterators));
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

    public PrestoSparkTaskProcessor<T> getTaskProcessor()
    {
        return taskProcessor;
    }
}
