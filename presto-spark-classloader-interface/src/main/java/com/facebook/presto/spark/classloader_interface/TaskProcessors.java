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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

// because of high number of template parameters inner classes are more readable then lambdas
@SuppressWarnings("Convert2Lambda")
public class TaskProcessors
{
    private TaskProcessors() {}

    public static PairFlatMapFunction<Iterator<SerializedPrestoSparkTaskDescriptor>, Integer, PrestoSparkRow> createTaskProcessor(
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
            // fragmentId -> Broadcast
            Map<String, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs)
    {
        return new PairFlatMapFunction<Iterator<SerializedPrestoSparkTaskDescriptor>, Integer, PrestoSparkRow>()
        {
            @Override
            public Iterator<Tuple2<Integer, PrestoSparkRow>> call(Iterator<SerializedPrestoSparkTaskDescriptor> serializedTaskRequestIterator)
            {
                SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = serializedTaskRequestIterator.next();
                if (serializedTaskRequestIterator.hasNext()) {
                    throw new IllegalArgumentException("each partition is expected to contain an exactly one task descriptor");
                }
                int partitionId = TaskContext.get().partitionId();
                int attemptNumber = TaskContext.get().attemptNumber();
                return taskExecutorFactoryProvider.get(SparkProcessType.EXECUTOR).create(
                        partitionId,
                        attemptNumber,
                        serializedTaskDescriptor,
                        new PrestoSparkTaskInputs(emptyMap(), broadcastInputs),
                        taskStatsCollector);
            }
        };
    }

    public static Function<List<Iterator<Tuple2<Integer, PrestoSparkRow>>>, Iterator<Tuple2<Integer, PrestoSparkRow>>> createTaskProcessor(
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            List<String> fragmentIds,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
            // fragmentId -> Broadcast
            Map<String, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs)
    {
        return new SerializableFunction<List<Iterator<Tuple2<Integer, PrestoSparkRow>>>, Iterator<Tuple2<Integer, PrestoSparkRow>>>()
        {
            @Override
            public Iterator<Tuple2<Integer, PrestoSparkRow>> apply(List<Iterator<Tuple2<Integer, PrestoSparkRow>>> iterators)
            {
                int partitionId = TaskContext.get().partitionId();
                int attemptNumber = TaskContext.get().attemptNumber();
                Map<String, Iterator<Tuple2<Integer, PrestoSparkRow>>> inputsMap = new HashMap<>();
                for (int i = 0; i < fragmentIds.size(); i++) {
                    inputsMap.put(fragmentIds.get(i), iterators.get(i));
                }
                return taskExecutorFactoryProvider.get(SparkProcessType.EXECUTOR).create(
                        partitionId,
                        attemptNumber,
                        serializedTaskDescriptor,
                        new PrestoSparkTaskInputs(unmodifiableMap(inputsMap), broadcastInputs),
                        taskStatsCollector);
            }
        };
    }

    public interface SerializableFunction<T, R>
            extends Function<T, R>, Serializable
    {
    }
}
