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

import com.facebook.presto.spi.function.AccumulatorStateFactory;

public class NDVEstimatorStateFactory
        implements AccumulatorStateFactory<NDVEstimatorState>
{
    public NDVEstimatorStateFactory() {}

    @Override
    public NDVEstimatorState createSingleState()
    {
        return new SingleNDVEstimatorState();
    }

    @Override
    public Class<? extends NDVEstimatorState> getSingleStateClass()
    {
        return SingleNDVEstimatorState.class;
    }

    @Override
    public NDVEstimatorState createGroupedState()
    {
        return new GroupNDVEstimatorState();
    }

    @Override
    public Class<? extends NDVEstimatorState> getGroupedStateClass()
    {
        return GroupNDVEstimatorState.class;
    }
}
