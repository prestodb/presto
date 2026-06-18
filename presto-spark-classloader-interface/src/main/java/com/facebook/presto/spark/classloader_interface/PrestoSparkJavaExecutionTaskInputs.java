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

import com.google.common.collect.ImmutableMap;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.collection.Iterator;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

// TODO: This is a temporary implementation intending to have a cleaner commit split. The next commit will move impl from PrestoSparkTaskInputs here.
public class PrestoSparkJavaExecutionTaskInputs
        implements PrestoSparkTaskInputs
{
    // fragmentId -> Iterator<[partitionId, page]>
    private final Map<String, Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputs;
    private final Map<String, Broadcast<?>> broadcastInputs;
    // For the COORDINATOR_ONLY fragment we first collect the inputs on the Driver
    private final Map<String, List<PrestoSparkSerializedPage>> inMemoryInputs;

    public PrestoSparkJavaExecutionTaskInputs(
            Map<String, Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputs,
            Map<String, Broadcast<?>> broadcastInputs,
            Map<String, List<PrestoSparkSerializedPage>> inMemoryInputs)
    {
        this.shuffleInputs = ImmutableMap.copyOf(requireNonNull(shuffleInputs, "shuffleInputs is null"));
        this.broadcastInputs = ImmutableMap.copyOf(requireNonNull(broadcastInputs, "broadcastInputs is null"));
        this.inMemoryInputs = ImmutableMap.copyOf(requireNonNull(inMemoryInputs, "inMemoryInputs is null"));
    }

    public Map<String, Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> getShuffleInputs()
    {
        return shuffleInputs;
    }

    public Map<String, Broadcast<?>> getBroadcastInputs()
    {
        return broadcastInputs;
    }

    public Map<String, List<PrestoSparkSerializedPage>> getInMemoryInputs()
    {
        return inMemoryInputs;
    }
}
