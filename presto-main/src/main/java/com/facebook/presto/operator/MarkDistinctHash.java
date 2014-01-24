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
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockBuilderStatus;
import com.facebook.presto.type.Type;

import java.util.List;

import static com.facebook.presto.type.BooleanType.BOOLEAN;

public class MarkDistinctHash
{
    private final GroupByHash groupByHash;
    private long nextDistinctId;

    public MarkDistinctHash(List<Type> types, int[] channels)
    {
        this(types, channels, 10_000);
    }

    public MarkDistinctHash(List<Type> types, int[] channels, int expectedDistinctValues)
    {
        this.groupByHash = new GroupByHash(types, channels, expectedDistinctValues);
    }

    public long getEstimatedSize()
    {
        return groupByHash.getEstimatedSize();
    }

    public Block markDistinctRows(Page page)
    {
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus());
        GroupByIdBlock ids = groupByHash.getGroupIds(page);
        for (int i = 0; i < ids.getPositionCount(); i++) {
            if (ids.getGroupId(i) == nextDistinctId) {
                blockBuilder.append(true);
                nextDistinctId++;
            }
            else {
                blockBuilder.append(false);
            }
        }

        return blockBuilder.build();
    }
}
