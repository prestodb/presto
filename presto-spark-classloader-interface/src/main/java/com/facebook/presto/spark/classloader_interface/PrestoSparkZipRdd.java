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
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static scala.collection.JavaConversions.asJavaIterator;
import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.asScalaIterator;
import static scala.collection.JavaConversions.seqAsJavaList;

public class PrestoSparkZipRdd
        extends ZippedPartitionsBaseRDD<Tuple2<Integer, PrestoSparkRow>>
{
    private List<RDD<Tuple2<Integer, PrestoSparkRow>>> rdds;
    private Function<List<Iterator<Tuple2<Integer, PrestoSparkRow>>>, Iterator<Tuple2<Integer, PrestoSparkRow>>> function;

    public PrestoSparkZipRdd(
            SparkContext context,
            List<RDD<Tuple2<Integer, PrestoSparkRow>>> rdds,
            Function<List<Iterator<Tuple2<Integer, PrestoSparkRow>>>, Iterator<Tuple2<Integer, PrestoSparkRow>>> function)
    {
        super(
                context,
                getRDDSequence(requireNonNull(rdds, "rdds is null")),
                false,
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
        this.rdds = rdds;
        this.function = context.clean(requireNonNull(function, "function is null"), true);
    }

    @SuppressWarnings("unchecked")
    private static Seq<RDD<?>> getRDDSequence(List<RDD<Tuple2<Integer, PrestoSparkRow>>> rdds)
    {
        return asScalaBuffer((List<RDD<?>>) (List<?>) new ArrayList<>(rdds)).toSeq();
    }

    @Override
    public scala.collection.Iterator<Tuple2<Integer, PrestoSparkRow>> compute(Partition split, TaskContext context)
    {
        List<Partition> partitions = seqAsJavaList(((ZippedPartitionsPartition) split).partitions());
        List<Iterator<Tuple2<Integer, PrestoSparkRow>>> iterators = new ArrayList<>();
        for (int i = 0; i < rdds.size(); i++) {
            iterators.add(asJavaIterator(rdds.get(i).iterator(partitions.get(i), context)));
        }
        return asScalaIterator(function.apply(unmodifiableList(iterators)));
    }

    @Override
    public void clearDependencies()
    {
        super.clearDependencies();
        rdds = null;
        function = null;
    }
}
