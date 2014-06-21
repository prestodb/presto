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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.stats.cardinality.HyperLogLog;

public class HyperLogLogStateSerializer
        implements AccumulatorStateSerializer<HyperLogLogState>
{
    @Override
    public void serialize(HyperLogLogState state, BlockBuilder out)
    {
        if (state.getHyperLogLog() == null) {
            out.appendNull();
        }
        else {
            out.appendSlice(state.getHyperLogLog().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, HyperLogLogState state)
    {
        if (!block.isNull(index)) {
            state.setHyperLogLog(HyperLogLog.newInstance(block.getSlice(index)));
        }
    }
}
