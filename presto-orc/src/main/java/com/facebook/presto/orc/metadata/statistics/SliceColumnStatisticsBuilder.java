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
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;

public interface SliceColumnStatisticsBuilder
        extends StatisticsBuilder
{
    @Override
    default void addBlock(Type type, Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                addValue(block, position);
            }
        }
    }

    default void addValue(Slice value)
    {
        addValue(value, 0, value.length());
    }

    void addValue(Slice value, int sourceIndex, int length);

    void addValue(Block block, int position);

    void addKnownValue(int sliceLength);
}
