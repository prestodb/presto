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
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

// because of high number of template parameters inner classes are more readable then lambdas
@SuppressWarnings("Convert2Lambda")
public class TaskProcessors
{
    private TaskProcessors() {}

    public static PairFlatMapFunction<Iterator<SerializedPrestoSparkTaskDescriptor>, Integer, PrestoSparkRow> createTaskProcessor(
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector)
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
                return taskExecutorFactoryProvider.get().create(partitionId, attemptNumber, serializedTaskDescriptor, new PrestoSparkTaskInputs(emptyMap()), taskStatsCollector);
            }
        };
    }

    public static PairFlatMapFunction<Iterator<Tuple2<Integer, PrestoSparkRow>>, Integer, PrestoSparkRow> createTaskProcessor(
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            String planNodeId,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector)
    {
        return new PairFlatMapFunction<Iterator<Tuple2<Integer, PrestoSparkRow>>, Integer, PrestoSparkRow>()
        {
            @Override
            public Iterator<Tuple2<Integer, PrestoSparkRow>> call(Iterator<Tuple2<Integer, PrestoSparkRow>> input)
            {
                int partitionId = TaskContext.get().partitionId();
                int attemptNumber = TaskContext.get().attemptNumber();
                return taskExecutorFactoryProvider.get().create(
                        partitionId,
                        attemptNumber,
                        serializedTaskDescriptor,
                        new PrestoSparkTaskInputs(singletonMap(planNodeId, input)),
                        taskStatsCollector);
            }
        };
    }

    public static FlatMapFunction2<Iterator<Tuple2<Integer, PrestoSparkRow>>, Iterator<Tuple2<Integer, PrestoSparkRow>>, Tuple2<Integer, PrestoSparkRow>> createTaskProcessor(
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            String planNodeId1,
            String planNodeId2,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector)
    {
        return new FlatMapFunction2<Iterator<Tuple2<Integer, PrestoSparkRow>>, Iterator<Tuple2<Integer, PrestoSparkRow>>, Tuple2<Integer, PrestoSparkRow>>()
        {
            @Override
            public Iterator<Tuple2<Integer, PrestoSparkRow>> call(
                    Iterator<Tuple2<Integer, PrestoSparkRow>> input1,
                    Iterator<Tuple2<Integer, PrestoSparkRow>> input2)
            {
                int partitionId = TaskContext.get().partitionId();
                int attemptNumber = TaskContext.get().attemptNumber();
                HashMap<String, Iterator<Tuple2<Integer, PrestoSparkRow>>> inputsMap = new HashMap<>();
                inputsMap.put(planNodeId1, input1);
                inputsMap.put(planNodeId2, input2);
                return taskExecutorFactoryProvider.get().create(
                        partitionId,
                        attemptNumber,
                        serializedTaskDescriptor,
                        new PrestoSparkTaskInputs(unmodifiableMap(inputsMap)),
                        taskStatsCollector);
            }
        };
    }
}
