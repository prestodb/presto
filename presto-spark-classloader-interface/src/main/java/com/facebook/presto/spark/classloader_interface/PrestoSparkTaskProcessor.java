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

import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class PrestoSparkTaskProcessor<T extends PrestoSparkTaskOutput>
        implements Serializable
{
    private final PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider;
    private final SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor;
    private final CollectionAccumulator<SerializedTaskInfo> taskInfoCollector;
    private final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;
    // fragmentId -> Broadcast
    private final Map<String, Broadcast<?>> broadcastInputs;
    private final Class<T> outputType;

    public PrestoSparkTaskProcessor(
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            Map<String, Broadcast<?>> broadcastInputs,
            Class<T> outputType)
    {
        this.taskExecutorFactoryProvider = requireNonNull(taskExecutorFactoryProvider, "taskExecutorFactoryProvider is null");
        this.serializedTaskDescriptor = requireNonNull(serializedTaskDescriptor, "serializedTaskDescriptor is null");
        this.taskInfoCollector = requireNonNull(taskInfoCollector, "taskInfoCollector is null");
        this.shuffleStatsCollector = requireNonNull(shuffleStatsCollector, "shuffleStatsCollector is null");
        this.broadcastInputs = new HashMap<>(requireNonNull(broadcastInputs, "broadcastInputs is null"));
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    public Iterator<Tuple2<MutablePartitionId, T>> process(
            Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources,
            // fragmentId -> Iterator<[partitionId, page]>
            Map<String, Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> shuffleInputs)
    {
        int partitionId = TaskContext.get().partitionId();
        int attemptNumber = TaskContext.get().attemptNumber();
        return taskExecutorFactoryProvider.get().create(
                partitionId,
                attemptNumber,
                serializedTaskDescriptor,
                serializedTaskSources,
                new PrestoSparkTaskInputs(shuffleInputs, broadcastInputs, emptyMap()),
                taskInfoCollector,
                shuffleStatsCollector,
                outputType);
    }
}
