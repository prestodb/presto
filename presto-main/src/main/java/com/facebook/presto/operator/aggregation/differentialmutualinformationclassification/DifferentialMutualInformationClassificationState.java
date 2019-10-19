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

import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy;
import com.facebook.presto.operator.aggregation.discreteentropy.DiscreteEntropyStateStrategy;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

import java.util.Map;

@AccumulatorStateMetadata(
        stateSerializerClass = DifferentialMutualInformationStateSerializer.class,
        stateFactoryClass = DifferentialMutualInformationClassificationStateFactory.class)
public interface DifferentialMutualInformationClassificationState
        extends AccumulatorState
{
    void setFeatureStrategy(DifferentialEntropyStateStrategy strategy);

    DifferentialEntropyStateStrategy getFeatureStrategy();

    void setOutcomeStrategy(DiscreteEntropyStateStrategy strategy);

    DiscreteEntropyStateStrategy getOutcomeStrategy();

    void setFeatureStrategyForOutcome(DifferentialEntropyStateStrategy strategy, int outcome);

    DifferentialEntropyStateStrategy getFeatureStrategyForOutcome(
            int outcome, DifferentialEntropyStateStrategy prototypeFeatureStrategy);

    void setFeatureStrategiesForOutcomes(Map<Integer, DifferentialEntropyStateStrategy> strategies);

    Map<Integer, DifferentialEntropyStateStrategy> getFeatureStrategiesForOutcomes();
}
