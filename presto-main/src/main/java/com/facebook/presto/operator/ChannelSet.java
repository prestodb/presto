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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;

public class ChannelSet
{
    private final GroupByHash hash;
    private final boolean containsNull;

    public ChannelSet(GroupByHash hash, boolean containsNull)
    {
        this.hash = hash;
        this.containsNull = containsNull;
    }

    public Type getType()
    {
        return hash.getTypes().get(0);
    }

    public long getEstimatedSizeInBytes()
    {
        return hash.getEstimatedSize();
    }

    public int size()
    {
        return hash.getGroupCount();
    }

    public boolean containsNull()
    {
        return containsNull;
    }

    public boolean contains(int position, Block hashBlock, Block block)
    {
        return hash.contains(position, hashBlock, block);
    }

    public static class ChannelSetBuilder
    {
        private final GroupByHash hash;
        private final OperatorContext operatorContext;
        private final Block nullBlock;
        private final Block nullHashBlock;

        public ChannelSetBuilder(Type type, int setChannel, int hashChannel, int expectedPositions, OperatorContext operatorContext)
        {
            this.hash = new GroupByHash(ImmutableList.of(type), new int[] {setChannel}, hashChannel, expectedPositions);
            this.operatorContext = operatorContext;
            this.nullBlock = type.createBlockBuilder(new BlockBuilderStatus()).appendNull().build();
            this.nullHashBlock = TypeUtils.getHashBlock(ImmutableList.of(type), nullBlock);
        }

        public ChannelSet build()
        {
            boolean setContainsNull = hash.contains(0, nullHashBlock, nullBlock);
            return new ChannelSet(hash, setContainsNull);
        }

        public long getEstimatedSize()
        {
            return hash.getEstimatedSize();
        }

        public int size()
        {
            return hash.getGroupCount();
        }

        public void addPage(Page page)
        {
            hash.getGroupIds(page);

            if (operatorContext != null) {
                operatorContext.setMemoryReservation(hash.getEstimatedSize());
            }
        }

        public void add(int position, Block hashBlock, Block block)
        {
            hash.putIfAbsent(position, hashBlock, block);
        }
    }
}
