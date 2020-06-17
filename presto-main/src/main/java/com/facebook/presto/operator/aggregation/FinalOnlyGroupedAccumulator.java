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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.GroupByIdBlock;

/**
 * {@link FinalOnlyGroupedAccumulator} is an accumulator that does not support partial aggregation
 * This is for spilling purposes so any underlying accumulator must support spilling
 */
public abstract class FinalOnlyGroupedAccumulator
        implements GroupedAccumulator
{
    @Override
    public final Type getIntermediateType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void evaluateIntermediate(int groupId, BlockBuilder output)
    {
        throw new UnsupportedOperationException();
    }
}
