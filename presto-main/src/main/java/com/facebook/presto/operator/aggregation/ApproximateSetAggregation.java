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

import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.stats.cardinality.HyperLogLog;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

public class ApproximateSetAggregation
        extends AbstractAggregationFunction<HyperLogLogState>
{
    private static final int NUMBER_OF_BUCKETS = 4096;
    private final Type parameterType;

    public ApproximateSetAggregation(Type parameterType)
    {
        super(HYPER_LOG_LOG, HYPER_LOG_LOG, parameterType);

        checkArgument(parameterType.equals(BIGINT) || parameterType.equals(DOUBLE) || parameterType.equals(VARCHAR),
                "Expected parameter type to be BIGINT, DOUBLE, or VARCHAR, but was %s",
                parameterType);

        this.parameterType = parameterType;
    }

    @Override
    protected void processInput(HyperLogLogState state, Block block, int index)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }

        state.addMemoryUsage(-hll.estimatedInMemorySize());
        add(block, index, parameterType, hll);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @Override
    protected void combineState(HyperLogLogState state, HyperLogLogState otherState)
    {
        HyperLogLog input = otherState.getHyperLogLog();

        HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @Override
    protected void evaluateFinal(HyperLogLogState state, BlockBuilder out)
    {
        getStateSerializer().serialize(state, out);
    }

    private static void add(Block block, int index, Type parameterType, HyperLogLog estimator)
    {
        if (parameterType.equals(BIGINT)) {
            estimator.add(block.getLong(index));
        }
        else if (parameterType.equals(DOUBLE)) {
            estimator.add(Double.doubleToLongBits(block.getDouble(index)));
        }
        else if (parameterType.equals(VARCHAR)) {
            estimator.add(block.getSlice(index));
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be BIGINT, DOUBLE, or VARCHAR");
        }
    }
}
