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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MergeJoinPageBuilder
{
    private final List<Type> types;
    private final List<BlockBuilder> builders;
    private int positionCount;
    private final long maxPositionCount = 1024;

    public static MergeJoinPageBuilder mergeJoinPageBuilder(List<Type> leftTypes, List<Type> rightTypes)
    {
        return new MergeJoinPageBuilder(leftTypes, rightTypes);
    }

    MergeJoinPageBuilder(List<Type> leftTypes, List<Type> rightTypes)
    {
        this.types = Stream.concat(ImmutableList.copyOf(leftTypes).stream(), ImmutableList.copyOf(rightTypes).stream()).collect(Collectors.toList());
        ImmutableList.Builder<BlockBuilder> builders = ImmutableList.builder();
        for (Type type : types) {
            builders.add(type.createBlockBuilder(null, 1024));
        }
        this.builders = builders.build();
    }

    public boolean isEmptyOutput()
    {
        return types.isEmpty();
    }

    public long getPositionCount()
    {
        return positionCount;
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public boolean isFull()
    {
        return positionCount == maxPositionCount;
    }

    public Page build()
    {
        Block[] blocks = new Block[builders.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = builders.get(i).build();
        }
        return new Page(positionCount, blocks);
    }

    public MergeJoinPageBuilder addRow(Page left, List<Integer> leftChannels, int leftIndex, Page right, List<Integer> rightChannels, int rightIndex)
    {
        if (isEmptyOutput()) {
            positionCount++;
            return this;
        }
        if (isNullMatch(left, leftChannels, leftIndex)) {
            return this;
        }

        for (int i = 0; i < leftChannels.size(); i++) {
            int channel = leftChannels.get(i);
            BlockBuilder blockBuilder = builders.get(i);
            types.get(i).appendTo(left.getBlock(channel), leftIndex, blockBuilder);
        }

        int offset = leftChannels.size();
        for (int j = 0; j < rightChannels.size(); j++) {
            int channel = rightChannels.get(j);
            BlockBuilder blockBuilder = builders.get(j + offset);
            types.get(j + offset).appendTo(right.getBlock(channel), rightIndex, blockBuilder);
        }
        positionCount++;
        return this;
    }

    public MergeJoinPageBuilder addRowForLeftJoin(Page left, List<Integer> leftChannels, int leftIndex)
    {
        if (isEmptyOutput()) {
            positionCount++;
            return this;
        }

        if (isNullMatch(left, leftChannels, leftIndex)) {
            return this;
        }

        for (int i = 0; i < leftChannels.size(); i++) {
            int channel = leftChannels.get(i);
            BlockBuilder blockBuilder = builders.get(i);
            types.get(i).appendTo(left.getBlock(channel), leftIndex, blockBuilder);
        }

        int offset = leftChannels.size();
        for (int j = 0; j < types.size() - offset; j++) {
            BlockBuilder blockBuilder = builders.get(j + offset);
            blockBuilder.appendNull();
        }
        positionCount++;
        return this;
    }

    public boolean isNullMatch(Page page, List<Integer> channels, int index)
    {
        for (int channel : channels) {
            if (page.getBlock(channel).isNull(index)) {
                return true;
            }
        }
        return false;
    }
}
