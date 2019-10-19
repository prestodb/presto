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
package com.facebook.presto.operator.aggregation.differentialmutualinformationclassification;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy;
import com.facebook.presto.operator.aggregation.discreteentropy.DiscreteEntropyStateStrategy;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.SizeOf;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DifferentialMutualInformationClassificationStateFactory
        implements AccumulatorStateFactory<DifferentialMutualInformationClassificationState>
{
    @Override
    public DifferentialMutualInformationClassificationState createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends DifferentialMutualInformationClassificationState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public DifferentialMutualInformationClassificationState createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends DifferentialMutualInformationClassificationState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements DifferentialMutualInformationClassificationState
    {
        private final ObjectBigArray<DifferentialEntropyStateStrategy> featureStrategies = new ObjectBigArray<>();
        private final ObjectBigArray<DiscreteEntropyStateStrategy> outcomeStrategies = new ObjectBigArray<>();
        ObjectBigArray<Map<Integer, DifferentialEntropyStateStrategy>> featureStrategiesPerOutcome = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            featureStrategies.ensureCapacity(size);
            outcomeStrategies.ensureCapacity(size);
            featureStrategiesPerOutcome.ensureCapacity(size);
        }

        @Override
        public void setFeatureStrategy(DifferentialEntropyStateStrategy strategy)
        {
            requireNonNull(strategy, "strategy is null");

            DifferentialEntropyStateStrategy previous = getFeatureStrategy();
            if (previous != null) {
                size -= previous.getEstimatedSize();
            }

            featureStrategies.set(getGroupId(), strategy);
            size += strategy.getEstimatedSize();
        }

        @Override
        public DifferentialEntropyStateStrategy getFeatureStrategy()
        {
            return featureStrategies.get(getGroupId());
        }

        @Override
        public void setOutcomeStrategy(DiscreteEntropyStateStrategy strategy)
        {
            requireNonNull(strategy, "strategy is null");

            DiscreteEntropyStateStrategy previous = getOutcomeStrategy();
            if (previous != null) {
                size -= previous.getEstimatedSize();
            }

            outcomeStrategies.set(getGroupId(), strategy);
            size += strategy.getEstimatedSize();
        }

        @Override
        public DiscreteEntropyStateStrategy getOutcomeStrategy()
        {
            return outcomeStrategies.get(getGroupId());
        }

        @Override
        public void setFeatureStrategyForOutcome(DifferentialEntropyStateStrategy strategy, int outcome)
        {
            requireNonNull(strategy, "strategy is null");

            DifferentialEntropyStateStrategy previous = featureStrategiesPerOutcome.get(getGroupId()).get(outcome);
            if (previous != null) {
                size -= previous.getEstimatedSize();
            }

            if (featureStrategiesPerOutcome.get(getGroupId()) == null) {
                featureStrategiesPerOutcome.set(getGroupId(), new HashMap<>());
            }
            featureStrategiesPerOutcome.get(getGroupId()).put(outcome, strategy);
            size += strategy.getEstimatedSize();
        }

        @Override
        public DifferentialEntropyStateStrategy getFeatureStrategyForOutcome(int outcome, DifferentialEntropyStateStrategy prototypeFeatureStrategy)
        {
            Map<Integer, DifferentialEntropyStateStrategy> relevantOutcomeStrategies = featureStrategiesPerOutcome.get(getGroupId());
            if (relevantOutcomeStrategies == null) {
                relevantOutcomeStrategies = new HashMap<>();
                featureStrategiesPerOutcome.set(getGroupId(), relevantOutcomeStrategies);
            }
            DifferentialEntropyStateStrategy strategy = relevantOutcomeStrategies.get(outcome);
            if (strategy == null) {
                strategy = prototypeFeatureStrategy.cloneEmpty();
                relevantOutcomeStrategies.put(outcome, strategy);
            }
            return strategy;
        }

        @Override
        public void setFeatureStrategiesForOutcomes(Map<Integer, DifferentialEntropyStateStrategy> strategies)
        {
            requireNonNull(strategies, "strategy is null");

            Map<Integer, DifferentialEntropyStateStrategy> previous = getFeatureStrategiesForOutcomes();
            if (previous != null) {
                size -= getEstimatedSizeOfFeatureStrategiesForOutcomes(previous);
            }

            this.featureStrategiesPerOutcome.set(getGroupId(), strategies);
            size += getEstimatedSizeOfFeatureStrategiesForOutcomes(strategies);
        }

        @Override
        public Map<Integer, DifferentialEntropyStateStrategy> getFeatureStrategiesForOutcomes()
        {
            return featureStrategiesPerOutcome.get(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return size + featureStrategies.sizeOf() + outcomeStrategies.sizeOf() + featureStrategiesPerOutcome.sizeOf();
        }
    }

    public static class SingleState
            implements DifferentialMutualInformationClassificationState
    {
        private DifferentialEntropyStateStrategy featureStrategy;
        private DiscreteEntropyStateStrategy outcomeStrategy;
        private Map<Integer, DifferentialEntropyStateStrategy> featureStrategiesPerOutcome;

        @Override
        public void setFeatureStrategy(DifferentialEntropyStateStrategy strategy)
        {
            requireNonNull(strategy, "strategy is null");

            this.featureStrategy = strategy;
        }

        @Override
        public DifferentialEntropyStateStrategy getFeatureStrategy()
        {
            return featureStrategy;
        }

        @Override
        public void setOutcomeStrategy(DiscreteEntropyStateStrategy strategy)
        {
            requireNonNull(strategy, "strategy is null");

            this.outcomeStrategy = strategy;
        }

        @Override
        public DiscreteEntropyStateStrategy getOutcomeStrategy()
        {
            return outcomeStrategy;
        }

        @Override
        public void setFeatureStrategyForOutcome(DifferentialEntropyStateStrategy strategy, int outcome)
        {
            requireNonNull(strategy, "strategy is null");
            featureStrategiesPerOutcome.put(outcome, strategy);
        }

        @Override
        public DifferentialEntropyStateStrategy getFeatureStrategyForOutcome(
            int outcome, DifferentialEntropyStateStrategy prototypeFeatureStrategy)
        {
            if (featureStrategiesPerOutcome == null) {
                featureStrategiesPerOutcome = new HashMap<>();
            }
            DifferentialEntropyStateStrategy strategy = featureStrategiesPerOutcome.get(outcome);
            if (strategy == null) {
                strategy = prototypeFeatureStrategy.cloneEmpty();
                featureStrategiesPerOutcome.put(outcome, strategy);
            }
            return strategy;
        }

        @Override
        public void setFeatureStrategiesForOutcomes(Map<Integer, DifferentialEntropyStateStrategy> outcomeStrategies)
        {
            requireNonNull(outcomeStrategies, "strategy is null");

            this.featureStrategiesPerOutcome = outcomeStrategies;
        }

        @Override
        public Map<Integer, DifferentialEntropyStateStrategy> getFeatureStrategiesForOutcomes()
        {
            return featureStrategiesPerOutcome;
        }

        @Override
        public long getEstimatedSize()
        {
            int size = 0;
            if (featureStrategy != null) {
                size += featureStrategy.getEstimatedSize() +
                        outcomeStrategy.getEstimatedSize() +
                        getEstimatedSizeOfFeatureStrategiesForOutcomes(featureStrategiesPerOutcome);
            }
            return size;
        }
    }

    private static long getEstimatedSizeOfFeatureStrategiesForOutcomes(Map<Integer, DifferentialEntropyStateStrategy> strategies)
    {
        if (strategies == null) {
            return 0;
        }
        return strategies.size() * SizeOf.SIZE_OF_INT +
                strategies.values().stream().mapToLong(DifferentialEntropyStateStrategy::getEstimatedSize).sum();
    }
}
