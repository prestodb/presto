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

import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.ParallelCollectionPartition;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spark.classloader_interface.PrestoSparkUtils.asScalaBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PrestoSparkTaskSourceRdd
        extends RDD<SerializedPrestoSparkTaskSource>
{
    /**
     * Each element in taskSourcesByPartitionId is a list of task sources assigned to the same Spark partition.
     * When input tables are unbucketed, task sources are distributed randomly across all partitions (tasks).
     * When input tables are bucketed, each bucket in task sources will be assigned to one Spark partition (task),
     * and the assignment is compatible to potential shuffle inputs.
     */
    private transient List<List<SerializedPrestoSparkTaskSource>> taskSourcesByPartitionId;

    public PrestoSparkTaskSourceRdd(SparkContext sparkContext, List<List<SerializedPrestoSparkTaskSource>> taskSourcesByPartitionId)
    {
        super(sparkContext, asScalaBuffer(Collections.<Dependency<?>>emptyList()).toSeq(), fakeClassTag());
        this.taskSourcesByPartitionId = requireNonNull(taskSourcesByPartitionId, "taskSourcesByPartitionId is null").stream()
                .map(ArrayList::new)
                .collect(toList());
    }

    private static <T> ClassTag<T> fakeClassTag()
    {
        return scala.reflect.ClassTag$.MODULE$.apply(SerializedPrestoSparkTaskSource.class);
    }

    @Override
    public Partition[] getPartitions()
    {
        Partition[] partitions = new Partition[taskSourcesByPartitionId.size()];
        for (int partitionId = 0; partitionId < taskSourcesByPartitionId.size(); partitionId++) {
            partitions[partitionId] = new ParallelCollectionPartition<>(
                    id(),
                    partitionId,
                    asScalaBuffer(taskSourcesByPartitionId.get(partitionId)).toSeq(),
                    fakeClassTag());
        }
        return partitions;
    }

    @Override
    public Iterator<SerializedPrestoSparkTaskSource> compute(Partition partition, TaskContext context)
    {
        ParallelCollectionPartition<SerializedPrestoSparkTaskSource> parallelCollectionPartition = toParallelCollectionPartition(partition);
        return new InterruptibleIterator<>(context, parallelCollectionPartition.iterator());
    }

    @SuppressWarnings("unchecked")
    private static <T> ParallelCollectionPartition<T> toParallelCollectionPartition(Partition partition)
    {
        return (ParallelCollectionPartition<T>) partition;
    }

    @Override
    public void clearDependencies()
    {
        super.clearDependencies();
        taskSourcesByPartitionId = null;
    }
}
