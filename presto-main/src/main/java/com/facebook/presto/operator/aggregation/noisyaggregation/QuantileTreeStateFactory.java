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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class QuantileTreeStateFactory
        implements AccumulatorStateFactory<QuantileTreeState>
{
    @Override
    public QuantileTreeState createSingleState()
    {
        return new SingleQuantileTreeState();
    }

    @Override
    public Class<? extends QuantileTreeState> getSingleStateClass()
    {
        return SingleQuantileTreeState.class;
    }

    @Override
    public QuantileTreeState createGroupedState()
    {
        return new GroupedQuantileTreeState();
    }

    @Override
    public Class<? extends QuantileTreeState> getGroupedStateClass()
    {
        return GroupedQuantileTreeState.class;
    }

    public static class GroupedQuantileTreeState
            implements QuantileTreeState, GroupedAccumulatorState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedQuantileTreeState.class).instanceSize();

        private final ObjectBigArray<Double> deltas = new ObjectBigArray<>();
        private final ObjectBigArray<Double> epsilons = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> probabilities = new ObjectBigArray<>();
        private final ObjectBigArray<QuantileTree> quantileTrees = new ObjectBigArray<>();

        private long retainedBytes;
        private long groupId;

        @Override
        public final void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        protected final long getGroupId()
        {
            return groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            deltas.ensureCapacity(size);
            epsilons.ensureCapacity(size);
            probabilities.ensureCapacity(size);
            quantileTrees.ensureCapacity(size);
        }

        @Override
        public QuantileTree getQuantileTree()
        {
            return quantileTrees.get(getGroupId());
        }

        @Override
        public double getDelta()
        {
            return deltas.get(getGroupId());
        }

        @Override
        public double getEpsilon()
        {
            return epsilons.get(getGroupId());
        }

        @Override
        public List<Double> getProbabilities()
        {
            return probabilities.get(getGroupId());
        }

        @Override
        public void setDelta(double value)
        {
            deltas.set(getGroupId(), value);
        }

        @Override
        public void setEpsilon(double value)
        {
            epsilons.set(getGroupId(), value);
        }

        @Override
        public void setProbabilities(List<Double> values)
        {
            probabilities.set(getGroupId(), values);
        }

        @Override
        public void setQuantileTree(QuantileTree tree)
        {
            requireNonNull(tree, "quantile tree is null");
            quantileTrees.set(getGroupId(), tree);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            retainedBytes += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + retainedBytes + quantileTrees.sizeOf() + deltas.sizeOf() + epsilons.sizeOf() + probabilities.sizeOf();
        }
    }

    public static class SingleQuantileTreeState
            implements QuantileTreeState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleQuantileTreeState.class).instanceSize();

        private double delta;
        private double epsilon;
        private List<Double> probabilities;
        private QuantileTree quantileTree;

        @Override
        public double getDelta()
        {
            return delta;
        }

        @Override
        public double getEpsilon()
        {
            return epsilon;
        }

        @Override
        public List<Double> getProbabilities()
        {
            return probabilities;
        }

        @Override
        public QuantileTree getQuantileTree()
        {
            return quantileTree;
        }

        @Override
        public void setDelta(double value)
        {
            delta = value;
        }

        @Override
        public void setEpsilon(double value)
        {
            epsilon = value;
        }

        @Override
        public void setProbabilities(List<Double> values)
        {
            probabilities = new ArrayList<>(values);
        }

        @Override
        public void setQuantileTree(QuantileTree tree)
        {
            quantileTree = tree;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (quantileTree != null) {
                estimatedSize += quantileTree.estimatedSizeInBytes();
            }
            return estimatedSize;
        }
    }
}
