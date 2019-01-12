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
package io.prestosql.plugin.ml;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import java.util.HashMap;
import java.util.Map;

public class EvaluateClassifierPredictionsStateFactory
        implements AccumulatorStateFactory<EvaluateClassifierPredictionsState>
{
    private static final long HASH_MAP_SIZE = ClassLayout.parseClass(HashMap.class).instanceSize();

    @Override
    public EvaluateClassifierPredictionsState createSingleState()
    {
        return new SingleEvaluateClassifierPredictionsState();
    }

    @Override
    public Class<? extends EvaluateClassifierPredictionsState> getSingleStateClass()
    {
        return SingleEvaluateClassifierPredictionsState.class;
    }

    @Override
    public EvaluateClassifierPredictionsState createGroupedState()
    {
        return new GroupedEvaluateClassifierPredictionsState();
    }

    @Override
    public Class<? extends EvaluateClassifierPredictionsState> getGroupedStateClass()
    {
        return GroupedEvaluateClassifierPredictionsState.class;
    }

    public static class GroupedEvaluateClassifierPredictionsState
            implements GroupedAccumulatorState, EvaluateClassifierPredictionsState
    {
        private final ObjectBigArray<Map<String, Integer>> truePositives = new ObjectBigArray<>();
        private final ObjectBigArray<Map<String, Integer>> falsePositives = new ObjectBigArray<>();
        private final ObjectBigArray<Map<String, Integer>> falseNegatives = new ObjectBigArray<>();
        private long groupId;
        private long memoryUsage;

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void addMemoryUsage(int memory)
        {
            memoryUsage += memory;
        }

        @Override
        public Map<String, Integer> getTruePositives()
        {
            if (truePositives.get(groupId) == null) {
                truePositives.set(groupId, new HashMap<>());
                memoryUsage += HASH_MAP_SIZE;
            }
            return truePositives.get(groupId);
        }

        @Override
        public Map<String, Integer> getFalsePositives()
        {
            if (falsePositives.get(groupId) == null) {
                falsePositives.set(groupId, new HashMap<>());
                memoryUsage += HASH_MAP_SIZE;
            }
            return falsePositives.get(groupId);
        }

        @Override
        public Map<String, Integer> getFalseNegatives()
        {
            if (falseNegatives.get(groupId) == null) {
                falseNegatives.set(groupId, new HashMap<>());
                memoryUsage += HASH_MAP_SIZE;
            }
            return falseNegatives.get(groupId);
        }

        @Override
        public void ensureCapacity(long size)
        {
            truePositives.ensureCapacity(size);
            falsePositives.ensureCapacity(size);
            falseNegatives.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return memoryUsage + truePositives.sizeOf() + falsePositives.sizeOf() + falseNegatives.sizeOf();
        }
    }

    public static class SingleEvaluateClassifierPredictionsState
            implements EvaluateClassifierPredictionsState
    {
        private final Map<String, Integer> truePositives = new HashMap<>();
        private final Map<String, Integer> falsePositives = new HashMap<>();
        private final Map<String, Integer> falseNegatives = new HashMap<>();
        private int memoryUsage;

        @Override
        public long getEstimatedSize()
        {
            return memoryUsage + 3 * HASH_MAP_SIZE;
        }

        @Override
        public void addMemoryUsage(int memory)
        {
            memoryUsage += memory;
        }

        @Override
        public Map<String, Integer> getTruePositives()
        {
            return truePositives;
        }

        @Override
        public Map<String, Integer> getFalsePositives()
        {
            return falsePositives;
        }

        @Override
        public Map<String, Integer> getFalseNegatives()
        {
            return falseNegatives;
        }
    }
}
