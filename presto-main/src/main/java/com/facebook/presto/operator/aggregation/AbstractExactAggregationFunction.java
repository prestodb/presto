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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractExactAggregationFunction<T extends AccumulatorState>
        extends AbstractAggregationFunction<T>
{
    protected AbstractExactAggregationFunction(Type finalType, Type intermediateType, Type parameterType)
    {
        super(finalType, intermediateType, parameterType, false);
    }

    @Override
    protected final void processInput(T state, Block block, int index, long sampleWeight)
    {
        processInput(state, block, index);
    }

    protected abstract void processInput(T state, Block block, int index);

    @Override
    protected final void evaluateFinal(T state, double confidence, BlockBuilder out)
    {
        evaluateFinal(state, out);
    }

    protected void evaluateFinal(T state, BlockBuilder out)
    {
        checkState(getFinalType() == BIGINT || getFinalType() == DOUBLE || getFinalType() == BOOLEAN || getFinalType() == VARCHAR);
        getStateSerializer().serialize(state, out);
    }
}
