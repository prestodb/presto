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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.TypeParameter;

public class ReservoirSampleStateFactory
        implements AccumulatorStateFactory<ReservoirSampleState>
{
    private final Type type;

    public ReservoirSampleStateFactory(@TypeParameter("T") Type type)
    {
        this.type = type;
    }

    @Override
    public ReservoirSampleState createSingleState()
    {
        return new SingleReservoirSampleState(type);
    }

    @Override
    public Class<? extends ReservoirSampleState> getSingleStateClass()
    {
        return SingleReservoirSampleState.class;
    }

    @Override
    public ReservoirSampleState createGroupedState()
    {
        return new GroupedReservoirSampleState();
    }

    @Override
    public Class<? extends ReservoirSampleState> getGroupedStateClass()
    {
        return GroupedReservoirSampleState.class;
    }
}
