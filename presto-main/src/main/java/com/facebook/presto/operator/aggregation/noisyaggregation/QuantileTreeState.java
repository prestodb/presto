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

import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

import java.util.List;

@AccumulatorStateMetadata(stateSerializerClass = QuantileTreeStateSerializer.class, stateFactoryClass = QuantileTreeStateFactory.class)
public interface QuantileTreeState
        extends AccumulatorState
{
    double getDelta();

    double getEpsilon();

    List<Double> getProbabilities();

    QuantileTree getQuantileTree();

    void setDelta(double delta);

    void setEpsilon(double epsilon);

    void setProbabilities(List<Double> probabilities);

    void setQuantileTree(QuantileTree tree);

    void addMemoryUsage(long value);
}
