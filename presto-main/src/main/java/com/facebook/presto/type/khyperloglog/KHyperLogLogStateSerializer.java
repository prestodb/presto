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

package com.facebook.presto.type.khyperloglog;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.type.khyperloglog.KHyperLogLogType.K_HYPER_LOG_LOG;

public class KHyperLogLogStateSerializer
        implements AccumulatorStateSerializer<KHyperLogLogState>
{
    @Override
    public Type getSerializedType()
    {
        return K_HYPER_LOG_LOG;
    }

    @Override
    public void serialize(KHyperLogLogState state, BlockBuilder out)
    {
        if (state.getKHLL() == null) {
            out.appendNull();
        }
        else {
            K_HYPER_LOG_LOG.writeSlice(out, state.getKHLL().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, KHyperLogLogState state)
    {
        state.setKHLL(KHyperLogLog.newInstance(K_HYPER_LOG_LOG.getSlice(block, index)));
    }
}
