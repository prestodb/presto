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

import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Murmur3;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

public class ApproximateCountDistinctAggregation
        extends AbstractExactAggregationFunction<SliceState>
{
    private static final HyperLogLog ESTIMATOR = new HyperLogLog(2048);

    private final Type parameterType;

    public ApproximateCountDistinctAggregation(Type parameterType)
    {
        super(BIGINT, VARCHAR, parameterType);

        checkArgument(parameterType == BIGINT || parameterType == DOUBLE || parameterType == VARCHAR,
                "Expected parameter type to be BIGINT, DOUBLE, or VARCHAR, but was %s",
                parameterType);

        this.parameterType = parameterType;
    }

    @Override
    protected void processInput(SliceState state, Block block, int index)
    {
        long hash = hash(block, index, parameterType);
        if (state.getSlice() == null) {
            state.setSlice(Slices.allocate(ESTIMATOR.getSizeInBytes()));
        }
        ESTIMATOR.update(hash, state.getSlice(), 0);
    }

    @Override
    protected void combineState(SliceState state, SliceState otherState)
    {
        if (state.getSlice() == null) {
            state.setSlice(otherState.getSlice());
        }
        else {
            ESTIMATOR.mergeInto(state.getSlice(), 0, otherState.getSlice(), 0);
        }
    }

    @Override
    protected void evaluateFinal(SliceState state, BlockBuilder out)
    {
        if (state.getSlice() != null) {
            out.appendLong(ESTIMATOR.estimate(state.getSlice(), 0));
        }
        else {
            out.appendLong(0);
        }
    }

    @VisibleForTesting
    public static double getStandardError()
    {
        return ESTIMATOR.getStandardError();
    }

    private static long hash(Block values, int index, Type parameterType)
    {
        if (parameterType == BIGINT) {
            long value = values.getLong(index);
            return Murmur3.hash64(value);
        }
        else if (parameterType == DOUBLE) {
            double value = values.getDouble(index);
            return Murmur3.hash64(Double.doubleToLongBits(value));
        }
        else if (parameterType == VARCHAR) {
            return Murmur3.hash64(values.getSlice(index));
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT, DOUBLE, or VARCHAR");
        }
    }
}
