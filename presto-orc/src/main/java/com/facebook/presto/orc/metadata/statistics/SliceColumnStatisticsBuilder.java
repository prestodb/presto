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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.AbstractVariableWidthType;
import com.facebook.presto.common.type.Type;

import static com.google.common.base.Preconditions.checkArgument;

public interface SliceColumnStatisticsBuilder
        extends StatisticsBuilder
{
    @Override
    default void addBlock(Type type, Block block)
    {
        checkArgument(type instanceof AbstractVariableWidthType, "type is not an AbstractVariableWidthType: %s", type);
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                addValue(block, position);
            }
        }
    }

    @Override
    default void addValue(Type type, Block block, int position)
    {
        checkArgument(type instanceof AbstractVariableWidthType, "type is not an AbstractVariableWidthType: %s", type);
        if (!block.isNull(position)) {
            addValue(block, position);
        }
    }

    void addValue(Block block, int position);
}
