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
package com.facebook.presto.spark.execution;

import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import scala.Tuple2;
import scala.collection.Iterator;

import static java.util.Objects.requireNonNull;

public class PrestoSparkShuffleInput
{
    private final int fragmentId;
    private final Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> iterator;

    public PrestoSparkShuffleInput(int fragmentId, Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> iterator)
    {
        this.fragmentId = fragmentId;
        this.iterator = requireNonNull(iterator, "iterator is null");
    }

    public int getFragmentId()
    {
        return fragmentId;
    }

    public Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> getIterator()
    {
        return iterator;
    }
}
