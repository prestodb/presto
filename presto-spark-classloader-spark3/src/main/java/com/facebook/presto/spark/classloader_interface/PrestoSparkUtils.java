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
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterable;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import java.util.Collection;
import java.util.List;

public class PrestoSparkUtils
{
    private PrestoSparkUtils()
    {
    }

    public static <T> Collection<T> asJavaCollection(Iterable<T> iterable)
    {
        return JavaConverters.asJavaCollection(iterable);
    }

    public static <T> List<T> seqAsJavaList(Seq<T> iterable)
    {
        return JavaConverters.seqAsJavaList(iterable);
    }

    public static <T> Buffer<T> asScalaBuffer(List<T> list)
    {
        return JavaConverters.asScalaBuffer(list);
    }

    public static Collection<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> getMapSizesByExecutorId(
            MapOutputTracker mapOutputTracker, int shuffleId, int startPartitionId, int endPartitionId)
    {
        return asJavaCollection(mapOutputTracker.getMapSizesByExecutorId(
            shuffleId, 0, Integer.MAX_VALUE, startPartitionId, endPartitionId).toList());
    }

    /**
     * Converts a Scala Seq to a Scala immutable Seq.
     *
     * @param seq the Scala sequence to convert
     * @param <T> the type of elements in the sequence
     * @return an immutable Scala sequence containing the same elements
     */
    public static <T> scala.collection.immutable.Seq<T> toImmutableSeq(Seq<T> seq)
    {
        if (seq instanceof scala.collection.immutable.Seq) {
            return (scala.collection.immutable.Seq<T>) seq;
        }
        else {
            List<T> javaList = seqAsJavaList(seq);
            return asScalaBuffer(javaList).toList();
        }
    }
}
