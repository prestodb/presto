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
package com.facebook.presto.operator.aggregation.estimatendv;

import com.facebook.presto.operator.aggregation.histogram.HistogramState;
import com.facebook.presto.operator.aggregation.histogram.HistogramStateSerializer;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateSerializerClass = HistogramStateSerializer.class,
        stateFactoryClass = NDVEstimatorStateFactory.class)
public interface NDVEstimatorState
        extends HistogramState
{
    double LOWER_BOUND_PROBABILITY = 0.1;

    long estimate();
}
