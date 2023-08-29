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
import com.facebook.presto.operator.aggregation.histogram.HistogramStateFactory;
import com.facebook.presto.operator.aggregation.histogram.HistogramStateSerializer;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

/**
 * Implementation of Chao's estimator from Chao 1984, using counts of values that appear exactly once and twice
 * Before running NDV estimator, first run a select count(*) group by col query to get the frequency of each
 * distinct value in col.
 * Collect the counts of values with each frequency and store in the Map field freqDict
 * Use the counts of values with frequency 1 and 2 to do the computation:
 * d_chao = d + (f_1)^2/(2*(f_2))
 * Return birthday problem solution if there are no values observed of frequency 2
 * Also make insane bets (10x) when every point observed is almost unique, which could be good or bad
 */
@AccumulatorStateMetadata(
        stateSerializerClass = HistogramStateSerializer.class,
        stateFactoryClass = HistogramStateFactory.class)
public interface NDVEstimatorState
        extends HistogramState
{
    double LOWER_BOUND_PROBABILITY = 0.1;

    long estimate();
}
