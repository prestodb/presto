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

import com.facebook.presto.Session;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class MarkDistinctHash
{
    private final GroupByHash groupByHash;
    private long nextDistinctId;

    public MarkDistinctHash(Session session, List<Type> types, int[] channels, Optional<Integer> hashChannel, JoinCompiler joinCompiler)
    {
        this(session, types, channels, hashChannel, 10_000, joinCompiler);
    }
    public MarkDistinctHash(Session session, List<Type> types, int[] channels, Optional<Integer> hashChannel, int expectedDistinctValues, JoinCompiler joinCompiler)
    {
        this.groupByHash = createGroupByHash(session, types, channels, hashChannel, expectedDistinctValues, joinCompiler);
    }

    public long getEstimatedSize()
    {
        return groupByHash.getEstimatedSize();
    }

    public Block markDistinctRows(Page page)
    {
        GroupByIdBlock ids = groupByHash.getGroupIds(page);
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), ids.getPositionCount());
        for (int i = 0; i < ids.getPositionCount(); i++) {
            if (ids.getGroupId(i) == nextDistinctId) {
                BOOLEAN.writeBoolean(blockBuilder, true);
                nextDistinctId++;
            }
            else {
                BOOLEAN.writeBoolean(blockBuilder, false);
            }
        }

        return blockBuilder.build();
    }
}
