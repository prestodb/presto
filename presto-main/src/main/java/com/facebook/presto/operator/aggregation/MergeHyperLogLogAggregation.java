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
import io.airlift.stats.cardinality.HyperLogLog;

import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;

public class MergeHyperLogLogAggregation
        extends AbstractSimpleAggregationFunction<HyperLogLogState>
{
    public MergeHyperLogLogAggregation()
    {
        super(HYPER_LOG_LOG, HYPER_LOG_LOG, HYPER_LOG_LOG);
    }

    @Override
    protected void processInput(HyperLogLogState state, Block block, int index)
    {
        HyperLogLog input = HyperLogLog.newInstance(block.getSlice(index));

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
}
