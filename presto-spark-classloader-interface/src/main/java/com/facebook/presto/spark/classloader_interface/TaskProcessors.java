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
import org.spark_project.guava.collect.Iterators;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

public class TaskProcessors
{
    private TaskProcessors() {}

    public static PairFlatMapFunction<Iterator<byte[]>, Integer, byte[]> createTaskProcessor(
            PrestoSparkTaskCompilerFactory taskCompilerFactory,
            CollectionAccumulator<byte[]> taskStatsCollector)
    {
        return new PairFlatMapFunction<Iterator<byte[]>, Integer, byte[]>() {
            /**
             * @return An iterator of tuple (partitionId, serializedPage)
             */
            @Override
            public Iterator<Tuple2<Integer, byte[]>> call(Iterator<byte[]> serializedTaskRequestIterator)
            {
                // Each partition contains exactly one task request
                byte[] serializedTaskDescriptor = Iterators.getOnlyElement(serializedTaskRequestIterator);

                int taskId = TaskContext.get().partitionId();
                return taskCompilerFactory.create().compile(taskId, serializedTaskDescriptor, emptyMap(), taskStatsCollector);
            }
        };
    }

    public static PairFlatMapFunction<Iterator<Tuple2<Integer, byte[]>>, Integer, byte[]> createTaskProcessor(
            PrestoSparkTaskCompilerFactory taskCompilerFactory,
            byte[] serializedTaskDescriptor,
            String inputId,
            CollectionAccumulator<byte[]> taskStatsCollector)
    {
        return (input) -> {
            int taskId = TaskContext.get().partitionId();
            return taskCompilerFactory.create().compile(taskId, serializedTaskDescriptor, singletonMap(inputId, input), taskStatsCollector);
        };
    }

    public static FlatMapFunction2<Iterator<Tuple2<Integer, byte[]>>, Iterator<Tuple2<Integer, byte[]>>, Tuple2<Integer, byte[]>> createTaskProcessor(
            PrestoSparkTaskCompilerFactory taskCompilerFactory,
            byte[] serializedTaskDescriptor,
            String inputId1,
            String inputId2,
            CollectionAccumulator<byte[]> taskStatsCollector)
    {
        return (input1, input2) -> {
            int taskId = TaskContext.get().partitionId();
            HashMap<String, Iterator<Tuple2<Integer, byte[]>>> inputsMap = new HashMap<>();
            inputsMap.put(inputId1, input1);
            inputsMap.put(inputId2, input2);
            return taskCompilerFactory.create().compile(taskId, serializedTaskDescriptor, unmodifiableMap(inputsMap), taskStatsCollector);
        };
    }
}
