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
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class PrestoSparkUtils
{
    private PrestoSparkUtils()
    {
    }

    public static <T> Collection<T> asJavaCollection(Iterable<T> iterable)
    {
        return JavaConversions.asJavaCollection(iterable);
    }

    public static <T> List<T> seqAsJavaList(Seq<T> iterable)
    {
        return JavaConversions.seqAsJavaList(iterable);
    }

    public static <T> Buffer<T> asScalaBuffer(List<T> list)
    {
        return JavaConversions.asScalaBuffer(list);
    }

    public static Collection<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> getMapSizesByExecutorId(
            MapOutputTracker mapOutputTracker, int shuffleId, int startPartitionId, int endPartitionId)
    {
        return convertCollection(asJavaCollection(mapOutputTracker.getMapSizesByExecutorId(
            shuffleId, startPartitionId, endPartitionId).toList()));
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

    /**
     * Utility method to convert a Collection of Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>> to
     * a Collection of Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>
     *
     * @param inputCollection The original Collection of Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>> elements
     * @return A Collection of Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>> elements
     */
    public static Collection<Tuple2<BlockManagerId, Seq<Tuple3<BlockId, Object, Object>>>> convertCollection(
            Collection<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> inputCollection)
    {
        return inputCollection.stream()
                .map(entry -> new Tuple2<>(entry._1(), convertTuple2SeqToTuple3Seq(entry._2(), null)))
                .collect(Collectors.toList());
    }

    /**
     * Utility method to convert a Seq of Tuple2 to a Seq of Tuple3 by adding a third element
     *
     * @param tuple2Seq The original Seq of Tuple2 elements
     * @param thirdElement The third element to add to each Tuple2
     * @return A Seq of Tuple3 elements with the third element added
     */
    public static Seq<Tuple3<BlockId, Object, Object>> convertTuple2SeqToTuple3Seq(Seq<Tuple2<BlockId, Object>> tuple2Seq, Object thirdElement)
    {
        List<Tuple3<BlockId, Object, Object>> tuple3List = seqAsJavaList(tuple2Seq).stream()
                .map(tuple2 -> new Tuple3<>(tuple2._1, tuple2._2, thirdElement))
                .collect(Collectors.toList());
        return asScalaBuffer(tuple3List).toSeq();
    }
}
