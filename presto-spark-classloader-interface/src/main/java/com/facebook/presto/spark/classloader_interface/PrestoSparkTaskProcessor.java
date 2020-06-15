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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrestoSparkTaskProcessor
        implements Serializable
{
    private final PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider;
    private final SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor;
    private final CollectionAccumulator<SerializedTaskStats> taskStatsCollector;
    // fragmentId -> Broadcast
    private final Map<String, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs;

    public PrestoSparkTaskProcessor(
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
            Map<String, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs)
    {
        this.taskExecutorFactoryProvider = requireNonNull(taskExecutorFactoryProvider, "taskExecutorFactoryProvider is null");
        this.serializedTaskDescriptor = requireNonNull(serializedTaskDescriptor, "serializedTaskDescriptor is null");
        this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
        this.broadcastInputs = new HashMap<>(requireNonNull(broadcastInputs, "broadcastInputs is null"));
    }

    public Iterator<Tuple2<MutablePartitionId, PrestoSparkRow>> process(
            Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources,
            // fragmentId -> Iterator<[partitionId, page]>
            Map<String, Iterator<Tuple2<MutablePartitionId, PrestoSparkRow>>> shuffleInputs)
    {
        int partitionId = TaskContext.get().partitionId();
        int attemptNumber = TaskContext.get().attemptNumber();
        return taskExecutorFactoryProvider.get(SparkProcessType.EXECUTOR).create(
                partitionId,
                attemptNumber,
                serializedTaskDescriptor,
                serializedTaskSources,
                new PrestoSparkTaskInputs(shuffleInputs, broadcastInputs),
                taskStatsCollector);
    }
}
