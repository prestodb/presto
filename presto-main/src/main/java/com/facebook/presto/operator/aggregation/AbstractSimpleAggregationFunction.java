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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

/**
 * An aggregation function which processes intermediate input the same
 * as normal input.
 */
public abstract class AbstractSimpleAggregationFunction<T extends AccumulatorState>
        extends AbstractAggregationFunction<T>
{
    protected AbstractSimpleAggregationFunction(Type finalType, Type intermediateType, Type parameterType)
    {
        super(finalType, intermediateType, parameterType);
    }

    @Override
    protected final void processIntermediate(T state, T scratchState, Block block, int index)
    {
        processInput(state, block, index);
    }

    @Override
    protected final void combineState(T state, T otherState)
    {
        // noop, since processIntermediate has been overridden.
    }
}
